package store

import (
	"bytes"
	"crypto/cipher"
	cryptorand "crypto/rand"
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/crypto/chacha20poly1305"
)

const objectPayloadMagic = "RPO1"

const (
	// maxDecodedObjectBytes is an explicit memory-safety boundary for one
	// canonical CAS object. Large files are chunked before they reach the codec,
	// so individual objects should remain comfortably below this limit.
	maxDecodedObjectBytes = 64 << 20
	// zstd framing overhead for a bounded input is small, but stored payloads get
	// their own conservative ceiling so a corrupted/local-tampered object file
	// cannot force an unbounded allocation before AEAD verification.
	maxStoredObjectBytes = 80 << 20
)

// Codec transforms canonical plaintext objects into Replay's encrypted local
// storage representation. Object identity is deliberately computed before
// compression and encryption. zstd's reusable encoder/decoder instances are
// protected explicitly so a LocalStore can safely serve concurrent readers and
// writers without relying on undocumented codec concurrency behavior.
type Codec struct {
	aead      cipher.AEAD
	encoder   *zstd.Encoder
	decoder   *zstd.Decoder
	encoderMu sync.Mutex
	decoderMu sync.Mutex
}

// NewCodec creates a storage codec using a 256-bit key supplied by Replay's
// key-management boundary. The codec does not persist or derive the master key.
func NewCodec(key []byte) (*Codec, error) {
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, fmt.Errorf("create XChaCha20-Poly1305: %w", err)
	}
	encoder, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderCRC(true),
		zstd.WithZeroFrames(true),
		zstd.WithEncoderConcurrency(1),
	)
	if err != nil {
		return nil, fmt.Errorf("create zstd encoder: %w", err)
	}
	decoder, err := zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderMaxMemory(maxDecodedObjectBytes),
	)
	if err != nil {
		_ = encoder.Close()
		return nil, fmt.Errorf("create zstd decoder: %w", err)
	}
	return &Codec{aead: aead, encoder: encoder, decoder: decoder}, nil
}

// Seal compresses and encrypts plaintext. The returned payload contains a
// versioned magic prefix followed by a fresh XChaCha nonce and ciphertext.
// The object ID is AEAD associated data, binding stored bytes to their identity.
func (c *Codec) Seal(plaintext []byte) (ObjectID, []byte, error) {
	if len(plaintext) > maxDecodedObjectBytes {
		return "", nil, fmt.Errorf("object plaintext is %d bytes, maximum is %d", len(plaintext), maxDecodedObjectBytes)
	}
	id := SumObject(plaintext)

	c.encoderMu.Lock()
	compressed := c.encoder.EncodeAll(plaintext, nil)
	c.encoderMu.Unlock()

	nonce := make([]byte, chacha20poly1305.NonceSizeX)
	if _, err := cryptorand.Read(nonce); err != nil {
		return "", nil, fmt.Errorf("generate object nonce: %w", err)
	}

	ciphertext := c.aead.Seal(nil, nonce, compressed, []byte(id))
	payload := make([]byte, 0, len(objectPayloadMagic)+len(nonce)+len(ciphertext))
	payload = append(payload, objectPayloadMagic...)
	payload = append(payload, nonce...)
	payload = append(payload, ciphertext...)
	if len(payload) > maxStoredObjectBytes {
		return "", nil, fmt.Errorf("encoded object is %d bytes, maximum stored size is %d", len(payload), maxStoredObjectBytes)
	}
	return id, payload, nil
}

// Open authenticates, decrypts, decompresses, and finally re-hashes an object.
// A successful AEAD check alone is not treated as sufficient CAS verification.
func (c *Codec) Open(expected ObjectID, payload []byte) ([]byte, error) {
	if len(payload) > maxStoredObjectBytes {
		return nil, fmt.Errorf("stored object exceeds %d-byte limit", maxStoredObjectBytes)
	}
	headerLen := len(objectPayloadMagic) + chacha20poly1305.NonceSizeX
	if len(payload) < headerLen+c.aead.Overhead() {
		return nil, fmt.Errorf("object payload too short")
	}
	if !bytes.Equal(payload[:len(objectPayloadMagic)], []byte(objectPayloadMagic)) {
		return nil, fmt.Errorf("unsupported object payload format")
	}

	nonceStart := len(objectPayloadMagic)
	nonceEnd := nonceStart + chacha20poly1305.NonceSizeX
	nonce := payload[nonceStart:nonceEnd]
	ciphertext := payload[nonceEnd:]

	compressed, err := c.aead.Open(nil, nonce, ciphertext, []byte(expected))
	if err != nil {
		return nil, fmt.Errorf("authenticate object payload: %w", err)
	}

	c.decoderMu.Lock()
	plaintext, err := c.decoder.DecodeAll(compressed, nil)
	c.decoderMu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("decompress object payload: %w", err)
	}
	if len(plaintext) > maxDecodedObjectBytes {
		return nil, fmt.Errorf("decoded object exceeds %d-byte limit", maxDecodedObjectBytes)
	}
	actual := SumObject(plaintext)
	if actual != expected {
		return nil, fmt.Errorf("object hash mismatch: got %s, want %s", actual, expected)
	}
	return plaintext, nil
}

// Close releases compressor resources. Callers must stop using the Codec before
// Close; concurrent Close with Seal/Open is not supported.
func (c *Codec) Close() error {
	c.encoderMu.Lock()
	defer c.encoderMu.Unlock()
	c.decoderMu.Lock()
	defer c.decoderMu.Unlock()
	c.decoder.Close()
	return c.encoder.Close()
}
