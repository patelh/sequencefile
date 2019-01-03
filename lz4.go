package sequencefile

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/patelh/golz4"
	"io"
)

type lz4Reader struct {
	r            io.Reader
	remaining    int
	compressed   bytes.Buffer
	uncompressed bytes.Buffer
	currentBlock *bytes.Reader
	sizeBuffer   []byte
}

const MAX_BUFFER_SIZE = 1024 * 1024

func newLz4Reader(r io.Reader) (*lz4Reader, error) {
	s := &lz4Reader{r: r, sizeBuffer:make([]byte, 4)}
	err := s.Reset(r)
	s.compressed.Grow(MAX_BUFFER_SIZE)
	s.uncompressed.Grow(MAX_BUFFER_SIZE)
	return s, err
}

func readLength(r io.Reader, readBuffer []byte) (int, error) {
	_, err := io.ReadAtLeast(r, readBuffer, len(readBuffer))
	if err != nil {
		return 0, err
	}

	return int(binary.BigEndian.Uint32(readBuffer)), nil
}

func (s *lz4Reader) Read(b []byte) (int, error) {
	// If anything is left over from a previous partial read, return that.
	if s.currentBlock != nil && s.currentBlock.Len() > 0 {
		return s.currentBlock.Read(b)
	} else {
		s.currentBlock = nil
	}

	if s.remaining <= 0 {
		return 0, io.EOF
	}

	compressedLength, err := readLength(s.r, s.sizeBuffer)
	if err != nil {
		return 0, err
	}
	if compressedLength == 0 {
		if s.remaining != 0 {
			return 0, errors.New("sequencefile: lz4: partial block")
		}
		return 0, io.EOF
	}

	s.compressed.Reset()
	_, err = io.CopyN(&s.compressed, s.r, int64(compressedLength))
	if err != nil {
		return 0, err
	}

	compressed := s.compressed.Bytes()

	s.uncompressed.Reset()
	uncompressed := s.uncompressed.Bytes()
	var uncompressedLength int
	uncompressedLength, err = s.decodeBlock(uncompressed[:s.remaining], compressed)
	if err != nil {
		return 0, err
	}
	s.remaining -= uncompressedLength
	if s.remaining < 0 {
		return 0, errors.New("sequencefile: lz4: partial block")
	}

	s.currentBlock = bytes.NewReader(uncompressed[:uncompressedLength])
	return s.currentBlock.Read(b)
}

func (s *lz4Reader) decodeBlock(uncompressed, compressed []byte) (int, error) {
	ulen, err := lz4.Uncompress(compressed, uncompressed)
	if err != nil {
		return 0, err
	}

	return ulen, nil
}

func (s *lz4Reader) Reset(r io.Reader) error {
	s.r = r
	s.uncompressed.Reset()
	s.compressed.Reset()

	var err error
	s.remaining, err = readLength(s.r, s.sizeBuffer)
	if err != nil {
		return errors.New("failed to read uncompressed length : " + err.Error())
	}
	if s.remaining < 0 {
		panic("sequencefile: lz4: stream size overflows int32")
	}

	return nil
}

func (s *lz4Reader) Close() error {
	return nil
}