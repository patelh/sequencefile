// Package sequencefile provides functionality for reading and writing Hadoop's
// SequenceFile format, documented here: http://goo.gl/sOSJmJ
package sequencefile

import "io"

type Compression int
type CompressionCodec int

const (
	SyncSize = 16

	GzipClassName   = "org.apache.hadoop.io.compress.GzipCodec"
	SnappyClassName = "org.apache.hadoop.io.compress.SnappyCodec"
	ZlibClassName   = "org.apache.hadoop.io.compress.DefaultCodec"
	Lz4ClassName   = "org.apache.hadoop.io.compress.Lz4Codec"
)

const (
	NoCompression Compression = iota + 1
	RecordCompression
	BlockCompression
)

const (
	GzipCompression CompressionCodec = iota + 1
	SnappyCompression
	ZlibCompression
	Lz4Compression
)

type decompressor interface {
	Read(p []byte) (n int, err error)
	Reset(r io.Reader) error
	Close() error
}
