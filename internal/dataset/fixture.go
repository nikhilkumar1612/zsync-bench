package dataset

import (
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

var fixtureTime = time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)

func WriteFixture(root, kind string) error {
	anchor := ChainTip{Height: 100, Hash: strings.Repeat("0", 64)}
	limits := Limits{MaxEventPayloadBytes: 1 << 20, MaxRawBlockBytes: 2 << 20, MaxReorgDepth: 100}
	writer, err := Create(root, "123e4567-e89b-12d3-a456-426614174000", anchor, limits, fixtureTime)
	if err != nil {
		return err
	}
	defer writer.Close()
	if _, err = writer.Append("source_started", SourceStartedEvent{SessionID: "fixture", RecorderVersion: "fixture-v1", ZebraEndpointID: "synthetic"}, fixtureTime); err != nil {
		return err
	}
	a, err := writer.ObserveBlock(makeFixtureBlock(anchor.Hash, 1, fixtureTime.Unix()), 101, fixtureTime.Unix(), fixtureTime)
	if err != nil {
		return err
	}
	if _, err = writer.Append("block_connected", BlockEvent{Block: a}, fixtureTime); err != nil {
		return err
	}
	if kind == "linear" {
		b, err := writer.ObserveBlock(makeFixtureBlock(a.Hash, 2, fixtureTime.Unix()+1), 102, fixtureTime.Unix()+1, fixtureTime)
		if err != nil {
			return err
		}
		if _, err = writer.Append("block_connected", BlockEvent{Block: b}, fixtureTime); err != nil {
			return err
		}
		if _, err = writer.Append("tip_changed", TipEvent{Tip: ChainTip{Height: b.Height, Hash: b.Hash}}, fixtureTime); err != nil {
			return err
		}
		return writer.Finalize(ChainTip{Height: b.Height, Hash: b.Hash}, fixtureTime)
	}
	if kind == "reorg" {
		b, err := writer.ObserveBlock(makeFixtureBlock(a.Hash, 2, fixtureTime.Unix()+1), 102, fixtureTime.Unix()+1, fixtureTime)
		if err != nil {
			return err
		}
		if _, err = writer.Append("block_connected", BlockEvent{Block: b}, fixtureTime); err != nil {
			return err
		}
		replacement, err := writer.ObserveBlock(makeFixtureBlock(a.Hash, 3, fixtureTime.Unix()+2), 102, fixtureTime.Unix()+2, fixtureTime)
		if err != nil {
			return err
		}
		if _, err = writer.Append("block_disconnected", BlockEvent{Block: b}, fixtureTime); err != nil {
			return err
		}
		if _, err = writer.Append("block_connected", BlockEvent{Block: replacement}, fixtureTime); err != nil {
			return err
		}
		if _, err = writer.Append("tip_changed", TipEvent{Tip: ChainTip{Height: replacement.Height, Hash: replacement.Hash}}, fixtureTime); err != nil {
			return err
		}
		return writer.Finalize(ChainTip{Height: replacement.Height, Hash: replacement.Hash}, fixtureTime)
	}
	return fmt.Errorf("unknown fixture kind %q", kind)
}

func makeFixtureBlock(parent string, nonce byte, blockTime int64) []byte {
	parentBytes, _ := hex.DecodeString(parent)
	raw := make([]byte, 1488)
	raw[0] = 4
	for i := 0; i < 32; i++ {
		raw[4+i] = parentBytes[31-i]
	}
	raw[100] = byte(blockTime)
	raw[101] = byte(blockTime >> 8)
	raw[102] = byte(blockTime >> 16)
	raw[103] = byte(blockTime >> 24)
	raw[104] = 2
	raw[108] = 3
	raw[139] = nonce
	raw[140] = 0xfd
	raw[141] = 0x40
	raw[142] = 0x05
	raw[1487] = 0
	return raw
}
