package zebra_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nikhilkumar1612/zsync-bench/internal/testzebra"
	"github.com/nikhilkumar1612/zsync-bench/internal/zebra"
)

func TestClientReadsZebraResponses(t *testing.T) {
	chain := testzebra.LinearChain(10, strings.Repeat("0", 64), 2, 1000)
	server := testzebra.New(chain)
	defer server.Close()
	client := newClient(t, server.URL(), time.Second, 4<<20)
	info, err := client.GetBlockchainInfo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.Chain != "test" || info.Blocks != 11 || info.BestBlockHash != chain[1].Hash || info.Headers == nil || *info.Headers != 11 || info.VerificationProgress == nil || *info.VerificationProgress != 1 || info.InitialBlockDownloadComplete != nil {
		t.Fatalf("unexpected info: %+v", info)
	}
	hash, err := client.GetBlockHash(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if hash != chain[0].Hash {
		t.Fatalf("hash %s, want %s", hash, chain[0].Hash)
	}
	raw, err := client.GetRawBlock(context.Background(), hash)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != string(chain[0].Raw) {
		t.Fatal("raw block mismatch")
	}
}

func TestClientAcceptsDirectRawHex(t *testing.T) {
	chain := testzebra.LinearChain(1, strings.Repeat("0", 64), 1, 1000)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ID uint64 `json:"id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&request)
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": fmtHex(chain[0].Raw)})
	}))
	defer server.Close()
	client := newClient(t, server.URL, time.Second, 4<<20)
	raw, err := client.GetRawBlock(context.Background(), chain[0].Hash)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != string(chain[0].Raw) {
		t.Fatal("raw block mismatch")
	}
}

func TestClientReturnsRPCError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ID uint64 `json:"id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&request)
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": nil, "error": map[string]any{"code": -8, "message": "height out of range"}})
	}))
	defer server.Close()
	client := newClient(t, server.URL, time.Second, 4<<20)
	_, err := client.GetBlockHash(context.Background(), 99)
	var rpcErr *zebra.RPCError
	if err == nil || !errorsAs(err, &rpcErr) || rpcErr.Code != -8 {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClientEnforcesTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { time.Sleep(100 * time.Millisecond) }))
	defer server.Close()
	client := newClient(t, server.URL, 10*time.Millisecond, 4<<20)
	if _, err := client.GetBlockchainInfo(context.Background()); err == nil {
		t.Fatal("expected timeout")
	}
}
func TestClientRejectsOversizedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(strings.Repeat("x", 2048))) }))
	defer server.Close()
	client := newClient(t, server.URL, time.Second, 1024)
	if _, err := client.GetBlockchainInfo(context.Background()); err == nil {
		t.Fatal("expected response size error")
	}
}

func newClient(t *testing.T, url string, timeout time.Duration, max int64) *zebra.Client {
	t.Helper()
	client, err := zebra.New(zebra.Config{URL: url, RequestTimeout: timeout, MaxResponseBytes: max})
	if err != nil {
		t.Fatal(err)
	}
	return client
}
func fmtHex(data []byte) string {
	const digits = "0123456789abcdef"
	result := make([]byte, len(data)*2)
	for i, value := range data {
		result[i*2] = digits[value>>4]
		result[i*2+1] = digits[value&15]
	}
	return string(result)
}
func errorsAs(err error, target any) bool { return errors.As(err, target) }
