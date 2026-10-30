package dataset

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

func BlockContentKey(hash string) (string, error) {
	if err := validateHash(hash); err != nil {
		return "", err
	}
	return "blocks/" + hash[:2] + "/" + hash[2:] + ".raw", nil
}

// ParseBlockIdentity extracts the previous block hash and computes the block
// identifier from a Zcash network-serialized block. It validates only the
// header boundary; transaction parsing belongs to the indexer work items.
func ParseBlockIdentity(raw []byte) (hash string, parentHash string, blockTime int64, err error) {
	const fixedHeader = 140
	if len(raw) <= fixedHeader {
		return "", "", 0, fmt.Errorf("raw block is too short: %d", len(raw))
	}
	if version := binary.LittleEndian.Uint32(raw[0:4]); version != 4 {
		return "", "", 0, fmt.Errorf("unsupported Zcash block version %d", version)
	}
	solutionLength, compactSizeLength, err := readCompactSize(raw[fixedHeader:])
	if err != nil {
		return "", "", 0, fmt.Errorf("read Equihash solution length: %w", err)
	}
	if solutionLength != 1344 {
		return "", "", 0, fmt.Errorf("invalid Equihash solution length %d, want 1344", solutionLength)
	}
	headerEnd := fixedHeader + compactSizeLength + int(solutionLength)
	if headerEnd >= len(raw) {
		return "", "", 0, fmt.Errorf("raw block ends before transaction count")
	}
	first := sha256.Sum256(raw[:headerEnd])
	second := sha256.Sum256(first[:])
	hash = reversedHex(second[:])
	parentHash = reversedHex(raw[4:36])
	blockTime = int64(binary.LittleEndian.Uint32(raw[100:104]))
	return hash, parentHash, blockTime, nil
}

func readCompactSize(data []byte) (uint64, int, error) {
	if len(data) == 0 {
		return 0, 0, fmt.Errorf("missing compact size")
	}
	switch data[0] {
	case 0xfd:
		if len(data) < 3 {
			return 0, 0, fmt.Errorf("truncated uint16 compact size")
		}
		v := uint64(data[1]) | uint64(data[2])<<8
		if v < 0xfd {
			return 0, 0, fmt.Errorf("non-canonical compact size")
		}
		return v, 3, nil
	case 0xfe:
		if len(data) < 5 {
			return 0, 0, fmt.Errorf("truncated uint32 compact size")
		}
		v := uint64(data[1]) | uint64(data[2])<<8 | uint64(data[3])<<16 | uint64(data[4])<<24
		if v <= 0xffff {
			return 0, 0, fmt.Errorf("non-canonical compact size")
		}
		return v, 5, nil
	case 0xff:
		if len(data) < 9 {
			return 0, 0, fmt.Errorf("truncated uint64 compact size")
		}
		var v uint64
		for i := 0; i < 8; i++ {
			v |= uint64(data[i+1]) << (8 * i)
		}
		if v <= 0xffffffff {
			return 0, 0, fmt.Errorf("non-canonical compact size")
		}
		return v, 9, nil
	default:
		return uint64(data[0]), 1, nil
	}
}

func reversedHex(data []byte) string {
	reversed := make([]byte, len(data))
	for i := range data {
		reversed[len(data)-1-i] = data[i]
	}
	return hex.EncodeToString(reversed)
}

func validateHash(value string) error {
	if len(value) != 64 {
		return fmt.Errorf("hash must contain 64 lowercase hex characters")
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 32 {
		return fmt.Errorf("invalid hash %q", value)
	}
	if hex.EncodeToString(decoded) != value {
		return fmt.Errorf("hash must use lowercase hexadecimal")
	}
	return nil
}
