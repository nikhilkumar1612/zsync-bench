package recorder

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"time"

	"github.com/nikhilkumar1612/zsync-bench/internal/dataset"
	"github.com/nikhilkumar1612/zsync-bench/internal/zebra"
)

type Source interface {
	GetBlockchainInfo(context.Context) (zebra.BlockchainInfo, error)
	GetBlockHash(context.Context, uint64) (string, error)
	GetRawBlock(context.Context, string) ([]byte, error)
}
type Clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}
type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

type Recorder struct {
	config Config
	source Source
	clock  Clock
	logger *slog.Logger
}

var errReevaluate = errors.New("source recovered; re-evaluate canonical chain")

const publicTestnetGenesisHash = "05a60a92d99d85997cce3b87616c089f6124d7342af37106edc76126334a2c38"

type runState struct {
	writer    *dataset.Writer
	tip       dataset.ChainTip
	anchor    dataset.ChainTip
	canonical map[uint64]dataset.BlockRef
	closed    bool
}

func New(config Config, source Source, logger *slog.Logger) *Recorder {
	if logger == nil {
		logger = slog.Default()
	}
	return &Recorder{config: config, source: source, clock: realClock{}, logger: logger}
}

func (r *Recorder) Run(ctx context.Context) error {
	if err := r.config.Validate(); err != nil {
		return err
	}
	state, err := r.open(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if !state.closed {
			_ = state.writer.Close()
		}
	}()
	sessionID := newUUID()
	if _, err := state.writer.Append("source_started", dataset.SourceStartedEvent{SessionID: sessionID, RecorderVersion: "zsync-recorder/0.1", ZebraEndpointID: sanitizeEndpoint(r.config.ZebraURL)}, r.clock.Now()); err != nil {
		return err
	}
	if err := writeCheckpoint(r.config.OutputDir, state.writer, state.tip, r.clock.Now()); err != nil {
		return err
	}

	var recorded uint64
	for {
		info, _, err := r.getInfo(ctx, state)
		if err != nil {
			if ctx.Err() != nil {
				return r.stop(state, sessionID, "context_cancelled")
			}
			return fmt.Errorf("refresh Zebra tip: %w", err)
		}
		if err := validateSourceIdentity(info); err != nil {
			return err
		}
		target := info.Blocks
		if r.config.MaxBlocks > 0 {
			remaining := r.config.MaxBlocks - recorded
			if remaining == 0 {
				return r.stop(state, sessionID, "max_blocks_reached")
			}
			if target > state.tip.Height+remaining {
				target = state.tip.Height + remaining
			}
		}
		changed, connected, err := r.reconcile(ctx, state, target)
		if errors.Is(err, errReevaluate) {
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return r.stop(state, sessionID, "context_cancelled")
			}
			return err
		}
		recorded += connected
		if changed {
			if err := writeCheckpoint(r.config.OutputDir, state.writer, state.tip, r.clock.Now()); err != nil {
				return err
			}
		}
		if r.config.MaxBlocks > 0 && recorded >= r.config.MaxBlocks {
			return r.stop(state, sessionID, "max_blocks_reached")
		}
		if !r.config.Follow {
			return r.stop(state, sessionID, "caught_up")
		}
		select {
		case <-ctx.Done():
			return r.stop(state, sessionID, "context_cancelled")
		case <-r.clock.After(r.config.PollInterval):
		}
	}
}

func (r *Recorder) open(ctx context.Context) (*runState, error) {
	if r.config.Resume {
		writer, resumed, err := dataset.OpenAppend(r.config.OutputDir)
		if err != nil {
			return nil, fmt.Errorf("resume dataset: %w", err)
		}
		info, err := r.source.GetBlockchainInfo(ctx)
		if err != nil {
			writer.Close()
			return nil, fmt.Errorf("inspect Zebra chain: %w", err)
		}
		if err := r.validateSourceForMode(info); err != nil {
			writer.Close()
			return nil, err
		}
		if err := r.verifyPublicTestnet(ctx); err != nil {
			writer.Close()
			return nil, err
		}
		if info.Blocks < resumed.Manifest.InitialAnchor.Height {
			writer.Close()
			return nil, fmt.Errorf("Zebra tip %d is behind dataset anchor %d", info.Blocks, resumed.Manifest.InitialAnchor.Height)
		}
		if err := validateRetainedRange(info, resumed.Tip.Height+1); err != nil {
			writer.Close()
			return nil, err
		}
		return &runState{writer: writer, tip: resumed.Tip, anchor: resumed.Manifest.InitialAnchor, canonical: resumed.Canonical}, nil
	}
	info, err := r.source.GetBlockchainInfo(ctx)
	if err != nil {
		return nil, fmt.Errorf("inspect Zebra chain: %w", err)
	}
	if err := r.validateSourceForMode(info); err != nil {
		return nil, err
	}
	if err := r.verifyPublicTestnet(ctx); err != nil {
		return nil, err
	}
	if info.Blocks < r.config.StartHeight-1 {
		return nil, fmt.Errorf("start height %d is beyond Zebra tip %d", r.config.StartHeight, info.Blocks)
	}
	if err := validateRetainedRange(info, r.config.StartHeight); err != nil {
		return nil, err
	}
	if !r.config.Follow {
		lastHeight := r.config.StartHeight + r.config.MaxBlocks - 1
		if lastHeight < r.config.StartHeight || lastHeight > info.Blocks {
			return nil, fmt.Errorf("requested fixed range ends at %d, but Zebra has committed through %d", lastHeight, info.Blocks)
		}
	}
	anchorHash, err := r.source.GetBlockHash(ctx, r.config.StartHeight-1)
	if err != nil {
		return nil, fmt.Errorf("fetch anchor at height %d: %w", r.config.StartHeight-1, err)
	}
	anchor := dataset.ChainTip{Height: r.config.StartHeight - 1, Hash: anchorHash}
	writer, err := dataset.Create(r.config.OutputDir, newUUID(), anchor, r.config.Limits, r.clock.Now())
	if err != nil {
		return nil, fmt.Errorf("create dataset: %w", err)
	}
	return &runState{writer: writer, tip: anchor, anchor: anchor, canonical: map[uint64]dataset.BlockRef{}}, nil
}

func validateSource(info zebra.BlockchainInfo) error {
	if err := validateSourceIdentity(info); err != nil {
		return err
	}
	if info.InitialBlockDownloadComplete != nil && !*info.InitialBlockDownloadComplete {
		return fmt.Errorf("Zebra initial block download is incomplete")
	}
	if info.InitialBlockDownloadComplete == nil {
		if info.Headers == nil || info.VerificationProgress == nil {
			return fmt.Errorf("Zebra readiness fields are unavailable")
		}
		if *info.VerificationProgress < 0.999 {
			return fmt.Errorf("Zebra verification progress %.6f is below 0.999", *info.VerificationProgress)
		}
		if *info.Headers > info.Blocks+100 {
			return fmt.Errorf("Zebra committed tip is %d blocks behind headers", *info.Headers-info.Blocks)
		}
	}
	return nil
}

func (r *Recorder) validateSourceForMode(info zebra.BlockchainInfo) error {
	if r.config.Follow {
		return validateSource(info)
	}
	return validateSourceIdentity(info)
}

func validateRetainedRange(info zebra.BlockchainInfo, startHeight uint64) error {
	if info.Pruned && info.PruneHeight != nil && startHeight < *info.PruneHeight {
		return fmt.Errorf("requested start height %d is below Zebra prune height %d", startHeight, *info.PruneHeight)
	}
	return nil
}

func validateSourceIdentity(info zebra.BlockchainInfo) error {
	if info.Chain != "test" {
		return fmt.Errorf("Zebra network is %q, want test", info.Chain)
	}
	return nil
}

func (r *Recorder) verifyPublicTestnet(ctx context.Context) error {
	genesis, err := r.source.GetBlockHash(ctx, 0)
	if err != nil {
		return fmt.Errorf("fetch Testnet genesis hash: %w", err)
	}
	if genesis != publicTestnetGenesisHash {
		return fmt.Errorf("Zebra genesis hash %s is not the public Zcash Testnet genesis", genesis)
	}
	return nil
}

// reconcile makes the recorded canonical chain equal Zebra through target. New
// branch objects become durable before any disconnect event is published.
func (r *Recorder) reconcile(ctx context.Context, state *runState, target uint64) (bool, uint64, error) {
	if target < state.anchor.Height {
		return false, 0, fmt.Errorf("Zebra tip %d is behind dataset anchor %d", target, state.anchor.Height)
	}
	ancestor := state.tip.Height
	if target < ancestor {
		ancestor = target
	}
	for ancestor > state.anchor.Height {
		sourceHash, recovered, err := r.getHash(ctx, state, ancestor)
		if err != nil {
			return false, 0, fmt.Errorf("find common ancestor at %d: %w", ancestor, err)
		}
		if recovered {
			return false, 0, errReevaluate
		}
		if local, ok := state.canonical[ancestor]; ok && local.Hash == sourceHash {
			break
		}
		ancestor--
		if state.tip.Height-ancestor > r.config.Limits.MaxReorgDepth {
			return false, 0, fmt.Errorf("reorganization exceeds maximum depth %d", r.config.Limits.MaxReorgDepth)
		}
	}
	if ancestor == state.anchor.Height {
		sourceHash, recovered, err := r.getHash(ctx, state, ancestor)
		if err != nil {
			return false, 0, fmt.Errorf("verify anchor at %d: %w", ancestor, err)
		}
		if recovered {
			return false, 0, errReevaluate
		}
		if sourceHash != state.anchor.Hash {
			return false, 0, fmt.Errorf("reorganization crosses dataset anchor at height %d", ancestor)
		}
	}

	branch := make([]dataset.BlockRef, 0, target-ancestor)
	parent := hashAt(state, ancestor)
	for height := ancestor + 1; height <= target; height++ {
		hash, recovered, err := r.getHash(ctx, state, height)
		if err != nil {
			return false, 0, fmt.Errorf("get block hash at %d: %w", height, err)
		}
		if recovered {
			return false, 0, errReevaluate
		}
		raw, recovered, err := r.getRaw(ctx, state, hash)
		if err != nil {
			return false, 0, fmt.Errorf("get raw block %s: %w", hash, err)
		}
		if recovered {
			return false, 0, errReevaluate
		}
		parsedHash, parsedParent, blockTime, err := dataset.ParseBlockIdentity(raw)
		if err != nil {
			return false, 0, fmt.Errorf("parse raw block at %d: %w", height, err)
		}
		if parsedHash != hash || parsedParent != parent {
			return false, 0, fmt.Errorf("invalid canonical block at height %d: hash %s parent %s", height, parsedHash, parsedParent)
		}
		ref, err := state.writer.ObserveBlock(raw, height, blockTime, r.clock.Now())
		if err != nil {
			return false, 0, err
		}
		branch = append(branch, ref)
		parent = hash
	}

	if ancestor == state.tip.Height && len(branch) == 0 {
		return false, 0, nil
	}
	for height := state.tip.Height; height > ancestor; height-- {
		ref := state.canonical[height]
		if _, err := state.writer.Append("block_disconnected", dataset.BlockEvent{Block: ref}, r.clock.Now()); err != nil {
			return false, 0, err
		}
		delete(state.canonical, height)
	}
	for _, ref := range branch {
		if _, err := state.writer.Append("block_connected", dataset.BlockEvent{Block: ref}, r.clock.Now()); err != nil {
			return false, 0, err
		}
		state.canonical[ref.Height] = ref
	}
	if len(branch) > 0 {
		last := branch[len(branch)-1]
		state.tip = dataset.ChainTip{Height: last.Height, Hash: last.Hash}
	} else {
		state.tip = dataset.ChainTip{Height: ancestor, Hash: hashAt(state, ancestor)}
	}
	if _, err := state.writer.Append("tip_changed", dataset.TipEvent{Tip: state.tip}, r.clock.Now()); err != nil {
		return false, 0, err
	}
	r.logger.Info("reconciled canonical chain", "height", state.tip.Height, "hash", state.tip.Hash, "connected_blocks", len(branch))
	return true, uint64(len(branch)), nil
}

func (r *Recorder) getInfo(ctx context.Context, state *runState) (zebra.BlockchainInfo, bool, error) {
	value, recovered, err := r.retry(ctx, state, func() (any, error) { return r.source.GetBlockchainInfo(ctx) })
	if err != nil {
		return zebra.BlockchainInfo{}, recovered, err
	}
	return value.(zebra.BlockchainInfo), recovered, nil
}

func (r *Recorder) getHash(ctx context.Context, state *runState, height uint64) (string, bool, error) {
	value, recovered, err := r.retry(ctx, state, func() (any, error) { return r.source.GetBlockHash(ctx, height) })
	if err != nil {
		return "", recovered, err
	}
	return value.(string), recovered, nil
}

func (r *Recorder) getRaw(ctx context.Context, state *runState, hash string) ([]byte, bool, error) {
	value, recovered, err := r.retry(ctx, state, func() (any, error) { return r.source.GetRawBlock(ctx, hash) })
	if err != nil {
		return nil, recovered, err
	}
	return value.([]byte), recovered, nil
}

func (r *Recorder) retry(ctx context.Context, state *runState, operation func() (any, error)) (any, bool, error) {
	delay := r.config.RetryMin
	var unavailableAt time.Time
	for {
		value, err := operation()
		if err == nil {
			if unavailableAt.IsZero() {
				return value, false, nil
			}
			_, appendErr := state.writer.Append("source_recovered", dataset.SourceRecoveredEvent{UnavailableSinceUnixNanos: unavailableAt.UnixNano()}, r.clock.Now())
			return value, true, appendErr
		}
		if unavailableAt.IsZero() {
			unavailableAt = r.clock.Now()
			_, appendErr := state.writer.Append("source_unavailable", dataset.SourceUnavailableEvent{Category: sourceFailureCategory(err), Diagnostic: "Zebra RPC temporarily unavailable"}, unavailableAt)
			if appendErr != nil {
				return nil, false, appendErr
			}
		}
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-r.clock.After(delay):
		}
		if delay < r.config.RetryMax {
			delay *= 2
			if delay > r.config.RetryMax {
				delay = r.config.RetryMax
			}
		}
	}
}

func sourceFailureCategory(err error) string {
	var netErr net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &netErr) && netErr.Timeout()) {
		return "timeout"
	}
	var rpcErr *zebra.RPCError
	if errors.As(err, &rpcErr) {
		return "invalid_response"
	}
	return "connection"
}

func hashAt(state *runState, height uint64) string {
	if height == state.anchor.Height {
		return state.anchor.Hash
	}
	return state.canonical[height].Hash
}

func (r *Recorder) stop(state *runState, sessionID, reason string) error {
	now := r.clock.Now()
	if _, err := state.writer.Append("source_stopped", dataset.SourceStoppedEvent{SessionID: sessionID, Reason: reason}, now); err != nil {
		return err
	}
	if err := writeCheckpoint(r.config.OutputDir, state.writer, state.tip, now); err != nil {
		return err
	}
	var err error
	if r.config.ShutdownMode == ShutdownFinalize {
		err = state.writer.Finalize(state.tip, now)
	} else {
		err = state.writer.Close()
	}
	if err == nil {
		state.closed = true
	}
	return err
}

func newUUID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic(err)
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(value[:])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32]
}

func sanitizeEndpoint(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "invalid"
	}
	return parsed.Scheme + "://" + parsed.Host
}
