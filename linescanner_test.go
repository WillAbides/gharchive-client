package gharchive

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/gzip"
	"github.com/stretchr/testify/require"
)

func Test_lineScanner(t *testing.T) {
	downloadTestFiles(t)
	file, err := os.Open(filepath.FromSlash("tmp/testfiles/short/2020-10-10-10.json.gz"))
	require.NoError(t, err)
	gzr, err := gzip.NewReader(file)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, gzr.Close())
		require.NoError(t, file.Close())
	})
	ls := &lineScanner{
		br: byteReader{
			r:    gzr,
			data: make([]byte, 0, newBufferSize),
		},
	}
	var count int
	for ls.scan() {
		if ls.error() != nil {
			break
		}
		require.True(t, json.Valid(ls.bytes()))
		count++
	}
	require.EqualError(t, ls.error(), "EOF")
	require.Equal(t, 10, count)
}
