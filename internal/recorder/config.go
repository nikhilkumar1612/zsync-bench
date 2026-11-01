package recorder

import (
	"fmt"
	"github.com/nikhilkumar1612/zsync-bench/internal/dataset"
	"time"
)

type ShutdownMode string

const (
	ShutdownPause    ShutdownMode = "pause"
	ShutdownFinalize ShutdownMode = "finalize"
)

type Config struct {
	ZebraURL         string
	Username         string
	Password         string
	OutputDir        string
	Resume           bool
	StartHeight      uint64
	MaxBlocks        uint64
	Follow           bool
	PollInterval     time.Duration
	RPCTimeout       time.Duration
	RetryMin         time.Duration
	RetryMax         time.Duration
	MaxResponseBytes int64
	ShutdownMode     ShutdownMode
	Limits           dataset.Limits
}

func DefaultConfig() Config {
	return Config{ZebraURL: "http://127.0.0.1:18232", PollInterval: 5 * time.Second, RPCTimeout: 30 * time.Second, RetryMin: time.Second, RetryMax: 30 * time.Second, MaxResponseBytes: 8 << 20, ShutdownMode: ShutdownPause, Limits: dataset.Limits{MaxEventPayloadBytes: 1 << 20, MaxRawBlockBytes: 2 << 20, MaxReorgDepth: 100}}
}
func (c Config) Validate() error {
	if c.ZebraURL == "" {
		return fmt.Errorf("Zebra URL is required")
	}
	if c.OutputDir == "" {
		return fmt.Errorf("output directory is required")
	}
	if !c.Resume && c.StartHeight == 0 {
		return fmt.Errorf("start height must be at least 1")
	}
	if !c.Follow && c.MaxBlocks == 0 {
		return fmt.Errorf("max blocks must be positive when follow mode is disabled")
	}
	if c.PollInterval <= 0 || c.RPCTimeout <= 0 {
		return fmt.Errorf("poll interval and RPC timeout must be positive")
	}
	if c.RetryMin <= 0 || c.RetryMax < c.RetryMin {
		return fmt.Errorf("retry minimum must be positive and no greater than retry maximum")
	}
	if c.ShutdownMode != ShutdownPause && c.ShutdownMode != ShutdownFinalize {
		return fmt.Errorf("shutdown mode must be pause or finalize")
	}
	if c.Limits.MaxEventPayloadBytes == 0 || c.Limits.MaxRawBlockBytes == 0 || c.Limits.MaxReorgDepth == 0 {
		return fmt.Errorf("dataset limits must be positive")
	}
	return nil
}
