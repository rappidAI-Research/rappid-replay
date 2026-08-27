package store

import (
	"bytes"
	"crypto/cipher"
	cryptorand "crypto/rand"
	"fmt"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/crypto/chacha20poly1305"
)

const objectPayloadMagic = "RPO1"

// Codec transforms canonical plaintext objects into Replay's encrypted local
// storage representation. Object identity is deliberately computed before
// compression and encryption.
type Codec struct {
	aead    cipher.AEAD
	encoder *zstd.Encoder
	decoder *zstd.Decoder
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
	)
	if err != nil {
		return nil, fmt.Errorf("create zstd encoder: %w", err)
	}
	decoder, err := zstd.NewReader(nil)
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
	id := SumObject(plaintext)
	compressed := c.encoder.EncodeAll(plaintext, nil)

	nonce := make([]byte, chacha20poly1305.NonceSizeX)
	if _, err := cryptorand.Read(nonce); err != nil {
		return "", nil, fmt.Errorf("generate object nonce: %w", err)
	}

	ciphertext := c.aead.Seal(nil, nonce, compressed, []byte(id))
	payload := make([]byte, 0, len(objectPayloadMagic)+len(nonce)+len(ciphertext))
	payload = append(payload, objectPayloadMagic...)
	payload = append(payload, nonce...)
	payload = append(payload, ciphertext...)
	return id, payload, nil
}

// Open authenticates, decrypts, decompresses, and finally re-hashes an object.
// A successful AEAD check alone is not treated as sufficient CAS verification.
func (c *Codec) Open(expected ObjectID, payload []byte) ([]byte, error) {
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
	plaintext, err := c.decoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("decompress object payload: %w", err)
	}
	actual := SumObject(plaintext)
	if actual != expected {
		return nil, fmt.Errorf("object hash mismatch: got %s, want %s", actual, expected)
	}
	return plaintext, nil
}

// Close releases compressor resources.
func (c *Codec) Close() error {
	c.decoder.Close()
	return c.encoder.Close()
}
