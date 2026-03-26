package repomofo

import (
	"bytes"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	gohash "hash"
)

type HashKind int

const (
	SHA1HashKind HashKind = iota
	SHA256HashKind
)

func (h HashKind) HashName() string {
	switch h {
	case SHA1HashKind:
		return "sha1"
	case SHA256HashKind:
		return "sha256"
	}
	panic("invalid hash kind")
}

func (h HashKind) ByteLen() int {
	switch h {
	case SHA1HashKind:
		return sha1.Size
	case SHA256HashKind:
		return sha256.Size
	}
	panic("invalid hash kind")
}

func (h HashKind) HexLen() int {
	return h.ByteLen() * 2
}

func (h HashKind) NewHasher() gohash.Hash {
	switch h {
	case SHA1HashKind:
		return sha1.New()
	case SHA256HashKind:
		return sha256.New()
	}
	panic("invalid hash kind")
}

func (h HashKind) HashBytes(data []byte) Hash {
	hasher := h.NewHasher()
	hasher.Write(data)
	return h.HashFromBytes(hasher.Sum(nil))
}

func (h HashKind) HexToBytes(hexStr string) ([]byte, error) {
	return hex.DecodeString(hexStr)
}

// Hash represents a git hash of either 20 bytes (SHA1) or 32 bytes (SHA256).
type Hash interface {
	Bytes() []byte
	Hex() string
	String() string
	ByteLen() int
	HexLen() int
	IsNull() bool
}

// SHA1Hash is a 20-byte SHA1 hash.
type SHA1Hash [20]byte

func (o SHA1Hash) Bytes() []byte  { return o[:] }
func (o SHA1Hash) Hex() string    { return hex.EncodeToString(o[:]) }
func (o SHA1Hash) String() string { return o.Hex() }
func (o SHA1Hash) ByteLen() int   { return 20 }
func (o SHA1Hash) HexLen() int    { return 40 }

func (o SHA1Hash) IsNull() bool {
	for _, b := range o {
		if b != 0 {
			return false
		}
	}
	return true
}

// SHA256Hash is a 32-byte SHA256 hash.
type SHA256Hash [32]byte

func (o SHA256Hash) Bytes() []byte  { return o[:] }
func (o SHA256Hash) Hex() string    { return hex.EncodeToString(o[:]) }
func (o SHA256Hash) String() string { return o.Hex() }
func (o SHA256Hash) ByteLen() int   { return 32 }
func (o SHA256Hash) HexLen() int    { return 64 }

func (o SHA256Hash) IsNull() bool {
	for _, b := range o {
		if b != 0 {
			return false
		}
	}
	return true
}

// HashFromBytes creates a Hash from a raw byte slice, using the hash kind to determine the type.
func (h HashKind) HashFromBytes(b []byte) Hash {
	switch h {
	case SHA1HashKind:
		var o SHA1Hash
		copy(o[:], b)
		return o
	case SHA256HashKind:
		var o SHA256Hash
		copy(o[:], b)
		return o
	}
	panic("invalid hash kind")
}

// HashFromHex creates a Hash from a hex string, using the hash kind to determine the type.
func (h HashKind) HashFromHex(s string) (Hash, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, err
	}
	return h.HashFromBytes(b), nil
}

// HashEqual compares two Hash values for equality. Nil-safe.
func HashEqual(a, b Hash) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return bytes.Equal(a.Bytes(), b.Bytes())
}

// NullHash returns the all-zeros Hash for the given hash kind.
func (h HashKind) NullHash() Hash {
	switch h {
	case SHA1HashKind:
		return SHA1Hash{}
	case SHA256HashKind:
		return SHA256Hash{}
	}
	panic("invalid hash kind")
}
