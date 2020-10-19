package gharchive

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func Benchmark_objReader(b *testing.B) {
	downloadTestFiles(b)
	bb, err := ioutil.ReadFile(filepath.FromSlash("tmp/testfiles/2020-10-10-8.json.gz"))
	require.NoError(b, err)
	brdr := bytes.NewReader(bb)
	o := new(objReader)
	var count int64
	b.ReportAllocs()
	uncompressedSize := int64(309426489)
	b.SetBytes(uncompressedSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = brdr.Seek(0, io.SeekStart)
		if err != nil {
			break
		}
		err = o.Reset(brdr)
		if err != nil {
			break
		}
		count, err = io.Copy(ioutil.Discard, o)
		if err != nil {
			break
		}
	}
	require.NoError(b, err)
	require.Equal(b, uncompressedSize, count)
	fmt.Println(count)
}
