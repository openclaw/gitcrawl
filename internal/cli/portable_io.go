package cli

import (
	"context"
	"crypto/sha256"
	"io"
	"os"
)

type portableValidationDirKey struct{}

type portableContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader portableContextReader) Read(data []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, context.Cause(reader.ctx)
	}
	return reader.reader.Read(data)
}

func portableFileSHA256(ctx context.Context, path string) ([32]byte, error) {
	var digest [32]byte
	file, err := os.Open(path)
	if err != nil {
		return digest, err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, portableContextReader{ctx: ctx, reader: file}); err != nil {
		return digest, err
	}
	copy(digest[:], hash.Sum(nil))
	return digest, nil
}
