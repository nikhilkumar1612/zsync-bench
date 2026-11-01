package zebra

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

const defaultMaxResponseBytes int64 = 8 << 20

type Config struct {
	URL              string
	Username         string
	Password         string
	RequestTimeout   time.Duration
	MaxResponseBytes int64
}

type Client struct {
	url              string
	username         string
	password         string
	timeout          time.Duration
	maxResponseBytes int64
	http             *http.Client
	nextID           atomic.Uint64
}

type BlockchainInfo struct {
	Chain                        string `json:"chain"`
	Blocks                       uint64 `json:"blocks"`
	BestBlockHash                string `json:"bestblockhash"`
	InitialBlockDownloadComplete bool   `json:"initial_block_download_complete"`
}

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *RPCError) Error() string { return fmt.Sprintf("zebra RPC error %d: %s", e.Code, e.Message) }

type request struct {
	JSONRPC string `json:"jsonrpc"`
	ID      uint64 `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params"`
}

type response struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result"`
	Error   *RPCError       `json:"error"`
	ID      uint64          `json:"id"`
}

func New(config Config) (*Client, error) {
	parsed, err := url.Parse(config.URL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, fmt.Errorf("invalid Zebra RPC URL %q", config.URL)
	}
	if config.RequestTimeout <= 0 {
		return nil, fmt.Errorf("request timeout must be positive")
	}
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = defaultMaxResponseBytes
	}
	if config.MaxResponseBytes < 1024 {
		return nil, fmt.Errorf("maximum response size must be at least 1024 bytes")
	}
	return &Client{url: config.URL, username: config.Username, password: config.Password, timeout: config.RequestTimeout, maxResponseBytes: config.MaxResponseBytes, http: &http.Client{}}, nil
}

func (c *Client) GetBlockchainInfo(ctx context.Context) (BlockchainInfo, error) {
	var result BlockchainInfo
	if err := c.call(ctx, "getblockchaininfo", []any{}, &result); err != nil {
		return BlockchainInfo{}, err
	}
	if result.Chain == "" || result.BestBlockHash == "" {
		return BlockchainInfo{}, fmt.Errorf("getblockchaininfo returned incomplete chain identity")
	}
	return result, nil
}

func (c *Client) GetBlockHash(ctx context.Context, height uint64) (string, error) {
	var result string
	if err := c.call(ctx, "getblockhash", []any{height}, &result); err != nil {
		return "", err
	}
	if err := validateHash(result); err != nil {
		return "", fmt.Errorf("getblockhash(%d): %w", height, err)
	}
	return result, nil
}

func (c *Client) GetRawBlock(ctx context.Context, hash string) ([]byte, error) {
	if err := validateHash(hash); err != nil {
		return nil, err
	}
	var raw json.RawMessage
	if err := c.callRaw(ctx, "getblock", []any{hash, 0}, &raw); err != nil {
		return nil, err
	}
	var encoded string
	if err := json.Unmarshal(raw, &encoded); err != nil {
		var object struct {
			Data string `json:"data"`
		}
		if objectErr := json.Unmarshal(raw, &object); objectErr != nil || object.Data == "" {
			return nil, fmt.Errorf("getblock returned unsupported raw response: %w", err)
		}
		encoded = object.Data
	}
	block, err := hex.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode getblock data: %w", err)
	}
	return block, nil
}

func (c *Client) call(ctx context.Context, method string, params any, target any) error {
	var raw json.RawMessage
	if err := c.callRaw(ctx, method, params, &raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode %s result: %w", method, err)
	}
	return nil
}

func (c *Client) callRaw(parent context.Context, method string, params any, target *json.RawMessage) error {
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	defer cancel()
	id := c.nextID.Add(1)
	body, err := json.Marshal(request{JSONRPC: "2.0", ID: id, Method: method, Params: params})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.username != "" || c.password != "" {
		req.SetBasicAuth(c.username, c.password)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("%s request timed out: %w", method, ctx.Err())
		}
		return fmt.Errorf("%s request failed: %w", method, err)
	}
	defer resp.Body.Close()
	limited := io.LimitReader(resp.Body, c.maxResponseBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return fmt.Errorf("read %s response: %w", method, err)
	}
	if int64(len(data)) > c.maxResponseBytes {
		return fmt.Errorf("%s response exceeds %d bytes", method, c.maxResponseBytes)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s returned HTTP %s", method, resp.Status)
	}
	var envelope response
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return fmt.Errorf("decode %s RPC envelope: %w", method, err)
	}
	if envelope.ID != id {
		return fmt.Errorf("%s response ID %d does not match request ID %d", method, envelope.ID, id)
	}
	if envelope.Error != nil {
		return envelope.Error
	}
	if len(envelope.Result) == 0 || bytes.Equal(envelope.Result, []byte("null")) {
		return fmt.Errorf("%s returned an empty result", method)
	}
	*target = envelope.Result
	return nil
}

func validateHash(value string) error {
	if len(value) != 64 {
		return fmt.Errorf("hash must have 64 hexadecimal characters")
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 32 {
		return fmt.Errorf("invalid hash %q", value)
	}
	if strings.ToLower(value) != value {
		return fmt.Errorf("hash must use lowercase hexadecimal")
	}
	return nil
}
