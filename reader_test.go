package sequencefile

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fileSpec struct {
	path        string
	compression Compression
	codec       CompressionCodec
	classname   string
	numRecords  int
}

var files = []fileSpec{
	{
		"testdata/uncompressed.sequencefile",
		NoCompression,
		0,
		"",
		2,
	},
	{
		"testdata/record_compressed_gzip.sequencefile",
		RecordCompression,
		GzipCompression,
		GzipClassName,
		2,
	},
	{
		"testdata/record_compressed_snappy.sequencefile",
		RecordCompression,
		SnappyCompression,
		SnappyClassName,
		2,
	},
	{
		"testdata/record_compressed_zlib.sequencefile",
		RecordCompression,
		ZlibCompression,
		ZlibClassName,
		2,
	},
	{
		"testdata/block_compressed_gzip.sequencefile",
		BlockCompression,
		GzipCompression,
		GzipClassName,
		2,
	},
	{
		"testdata/block_compressed_snappy.sequencefile",
		BlockCompression,
		SnappyCompression,
		SnappyClassName,
		2,
	},
	{
		"testdata/block_compressed_zlib.sequencefile",
		BlockCompression,
		ZlibCompression,
		ZlibClassName,
		2,
	},
	{
		"testdata/block_compressed_lz4_raw.sequencefile",
		BlockCompression,
		Lz4Compression,
		Lz4ClassName,
		2,
	},
	{
		"testdata/block_compressed_lz4.sequencefile",
		BlockCompression,
		Lz4Compression,
		Lz4ClassName,
		102,
	},
}

func TestReadFile(t *testing.T) {
	for _, spec := range files {
		t.Run(spec.path, func(t *testing.T) {
			file, err := os.Open(spec.path)
			require.NoError(t, err)

			r := NewReader(file)
			err = r.ReadHeader()
			require.NoError(t, err, "reading the header should succeed")

			testFileSpec(t, r, spec)
		})
	}
}

func testFileSpec(t *testing.T, r *Reader, spec fileSpec) {
	assert.Equal(t, 6, r.Header.Version, "The version should be set")
	assert.Equal(t, "org.apache.hadoop.io.BytesWritable", r.Header.KeyClassName, "The key class name should be set")
	assert.Equal(t, "org.apache.hadoop.io.BytesWritable", r.Header.ValueClassName, "The value class name should be set")
	assert.Equal(t, map[string]string{}, r.Header.Metadata, "The metadata should be set")

	assert.Equal(t, spec.compression, r.Header.Compression, "The compression should be set")
	assert.Equal(t, spec.codec, r.Header.CompressionCodec, "The compression codec should be set")
	assert.Equal(t, spec.classname, r.Header.CompressionCodecClassName, "The compression codec should be set")

	file := r.reader.(*os.File)
	offset1, _ := file.Seek(0, os.SEEK_CUR)
	ok := r.Scan()
	require.NoError(t, r.Err(), "ScanKey should succeed")
	require.True(t, ok, "ScanKey should succeed")

	assert.Equal(t, "Alice", string(BytesWritable(r.Key())), "The key should be correct")
	assert.Equal(t, "Practice", string(BytesWritable(r.Value())), "The value should be correct")

	ok = r.Scan()
	require.NoError(t, r.Err(), "Scan should succeed")
	require.True(t, ok, "Scan should succeed")

	assert.Equal(t, "Bob", string(BytesWritable(r.Key())), "The key should be correct")
	assert.Equal(t, "Hope", string(BytesWritable(r.Value())), "The value should be correct")

	// EOF
	for i:=0; i< (spec.numRecords - 2); i++ {
		ok = r.Scan()
		require.True(t, ok, "Scan should succeed to read all records")
	}
	ok = r.Scan()
	require.NoError(t, r.Err(), "Scan at the end of the file should fail without an error")
	require.False(t, ok, "Scan at the end of the file should fail without an error")

	file.Seek(offset1, os.SEEK_SET)
	r.Reset()
	ok = r.Scan()
	require.NoError(t, r.Err(), "Scan should succeed")
	require.True(t, ok, "Scan should succeed")

	assert.Equal(t, "Alice", string(BytesWritable(r.Key())), "The key should be correct")
	assert.Equal(t, "Practice", string(BytesWritable(r.Value())), "The value should be correct")
}
