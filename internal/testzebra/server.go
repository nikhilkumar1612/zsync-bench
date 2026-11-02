package testzebra

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"

	"github.com/nikhilkumar1612/zsync-bench/internal/dataset"
)

type Block struct {
	Height     uint64
	Hash       string
	ParentHash string
	Time       int64
	Raw        []byte
}
type Server struct {
	mu           sync.RWMutex
	http         *httptest.Server
	chain        []Block
	byHash       map[string]Block
	requests     map[string]int
	failures     map[string]int
	genesis      string
	headersAhead uint64
	progress     float64
	pruned       bool
	pruneHeight  *uint64
}

func New(chain []Block) *Server {
	s := &Server{chain: append([]Block(nil), chain...), requests: map[string]int{}, failures: map[string]int{}, genesis: "05a60a92d99d85997cce3b87616c089f6124d7342af37106edc76126334a2c38", progress: 1}
	s.reindex()
	s.http = httptest.NewServer(http.HandlerFunc(s.handle))
	return s
}
func (s *Server) Close()      { s.http.Close() }
func (s *Server) URL() string { return s.http.URL }
func (s *Server) SetChain(chain []Block) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.chain = append([]Block(nil), chain...)
	s.reindex()
}
func (s *Server) RequestCount(method string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.requests[method]
}
func (s *Server) FailNext(method string, count int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failures[method] += count
}
func (s *Server) SetGenesisHash(hash string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.genesis = hash
}
func (s *Server) SetSyncState(headersAhead uint64, progress float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.headersAhead = headersAhead
	s.progress = progress
}
func (s *Server) SetPruning(pruneHeight uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruned = true
	s.pruneHeight = &pruneHeight
}
func (s *Server) reindex() {
	s.byHash = map[string]Block{}
	for _, block := range s.chain {
		s.byHash[block.Hash] = block
	}
}

type rpcRequest struct {
	JSONRPC string            `json:"jsonrpc"`
	ID      uint64            `json:"id"`
	Method  string            `json:"method"`
	Params  []json.RawMessage `json:"params"`
}
type rpcResponse struct {
	JSONRPC string `json:"jsonrpc"`
	Result  any    `json:"result,omitempty"`
	Error   any    `json:"error,omitempty"`
	ID      uint64 `json:"id"`
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	var request rpcRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "bad request", 400)
		return
	}
	s.mu.Lock()
	s.requests[request.Method]++
	if s.failures[request.Method] > 0 {
		s.failures[request.Method]--
		s.mu.Unlock()
		_ = json.NewEncoder(w).Encode(rpcResponse{JSONRPC: "2.0", Error: map[string]any{"code": -1, "message": "injected temporary failure"}, ID: request.ID})
		return
	}
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	result, code, message := s.dispatch(request)
	response := rpcResponse{JSONRPC: "2.0", Result: result, ID: request.ID}
	if code != 0 {
		response.Result = nil
		response.Error = map[string]any{"code": code, "message": message}
	}
	_ = json.NewEncoder(w).Encode(response)
}
func (s *Server) dispatch(request rpcRequest) (any, int, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.chain) == 0 {
		return nil, -1, "empty chain"
	}
	switch request.Method {
	case "getblockchaininfo":
		tip := s.chain[len(s.chain)-1]
		result := map[string]any{"chain": "test", "blocks": tip.Height, "headers": tip.Height + s.headersAhead, "estimatedheight": tip.Height + s.headersAhead, "verificationprogress": s.progress, "bestblockhash": tip.Hash, "pruned": s.pruned, "zakura_extension": true}
		if s.pruneHeight != nil {
			result["pruneheight"] = *s.pruneHeight
		}
		return result, 0, ""
	case "getblockhash":
		if len(request.Params) != 1 {
			return nil, -32602, "invalid params"
		}
		var height uint64
		if err := json.Unmarshal(request.Params[0], &height); err != nil {
			return nil, -32602, "invalid height"
		}
		for _, block := range s.chain {
			if block.Height == height {
				return block.Hash, 0, ""
			}
		}
		if height == 0 {
			return s.genesis, 0, ""
		}
		return nil, -8, "height out of range"
	case "getblock":
		if len(request.Params) < 1 {
			return nil, -32602, "invalid params"
		}
		var hash string
		if err := json.Unmarshal(request.Params[0], &hash); err != nil {
			return nil, -32602, "invalid hash"
		}
		block, ok := s.byHash[hash]
		if !ok {
			return nil, -5, "block not found"
		}
		return map[string]any{"data": hex.EncodeToString(block.Raw)}, 0, ""
	default:
		return nil, -32601, "method not found"
	}
}

func LinearChain(startHeight uint64, parentHash string, count int, startTime int64) []Block {
	return Chain(startHeight, parentHash, count, startTime, 1)
}

// Chain builds a deterministic synthetic branch. A distinct firstNonce creates
// a distinct branch even when height, parent and timestamp are the same.
func Chain(startHeight uint64, parentHash string, count int, startTime int64, firstNonce byte) []Block {
	chain := make([]Block, 0, count)
	parent := parentHash
	for i := 0; i < count; i++ {
		height := startHeight + uint64(i)
		raw := makeBlock(parent, firstNonce+byte(i), startTime+int64(i))
		hash, parsedParent, blockTime, err := dataset.ParseBlockIdentity(raw)
		if err != nil {
			panic(err)
		}
		chain = append(chain, Block{Height: height, Hash: hash, ParentHash: parsedParent, Time: blockTime, Raw: raw})
		parent = hash
	}
	return chain
}
func makeBlock(parent string, nonce byte, blockTime int64) []byte {
	parentBytes, err := hex.DecodeString(parent)
	if err != nil {
		panic(err)
	}
	raw := make([]byte, 1488)
	raw[0] = 4
	for i := 0; i < 32; i++ {
		raw[4+i] = parentBytes[31-i]
	}
	raw[100] = byte(blockTime)
	raw[101] = byte(blockTime >> 8)
	raw[102] = byte(blockTime >> 16)
	raw[103] = byte(blockTime >> 24)
	raw[139] = nonce
	raw[140] = 0xfd
	raw[141] = 0x40
	raw[142] = 0x05
	raw[1487] = 0
	return raw
}
