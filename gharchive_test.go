package gharchive

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/klauspost/compress/gzip"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

var testfiles = []string{
	"2020-10-10-8.json.gz",
	"2020-10-10-9.json.gz",
	"2020-10-10-10.json.gz",
}

func downloadTestFiles(t testing.TB) {
	t.Helper()

	dir := filepath.FromSlash("./tmp/testfiles")
	shortDir := filepath.Join(dir, "short")
	err := os.MkdirAll(shortDir, 0o700)
	require.NoError(t, err)
	for _, filename := range testfiles {
		_, err = os.Stat(filepath.Join(dir, filename))
		if err == nil {
			continue
		}
		if os.IsNotExist(err) {
			err = nil
		}
		require.NoError(t, err)
		var resp *http.Response
		resp, err = http.Get("https://data.gharchive.org/" + filename)
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode)
		var file *os.File
		file, err = os.Create(filepath.Join(dir, filename))
		require.NoError(t, err)
		_, err = io.Copy(file, resp.Body)
		require.NoError(t, file.Close())
		require.NoError(t, resp.Body.Close())
		require.NoError(t, err)
	}
	for _, filename := range testfiles {
		_, err = os.Stat(filepath.Join(shortDir, filename))
		if err == nil {
			continue
		}
		if os.IsNotExist(err) {
			err = nil
		}
		require.NoError(t, err)
		infile, err := os.Open(filepath.Join(dir, filename))
		require.NoError(t, err)
		gzr, err := gzip.NewReader(infile)
		require.NoError(t, err)
		outfile, err := os.Create(filepath.Join(shortDir, filename))
		require.NoError(t, err)
		gzw := gzip.NewWriter(outfile)
		ls := lineScanner{
			br: byteReader{
				r: gzr,
			},
		}
		for i := 0; i < 10; i++ {
			ls.scan()
			b := ls.bytes()
			if i == 9 {
				b = bytes.TrimSpace(b)
			}
			_, err = gzw.Write(b)
			require.NoError(t, err)
			require.NoError(t, ls.error())
		}
		require.NoError(t, gzw.Close())
		require.NoError(t, gzr.Close())
		require.NoError(t, infile.Close())
		require.NoError(t, outfile.Close())
	}
}

func setupTestServer(ctx context.Context, t *testing.T) *storage.Client {
	t.Helper()
	downloadTestFiles(t)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		http.ServeFile(w, req, filepath.Join("tmp", "testfiles", path.Base(req.URL.Path)))
	}))
	t.Cleanup(func() {
		server.Close()
	})
	client, err := storage.NewClient(ctx,
		option.WithoutAuthentication(),
		option.WithEndpoint(server.URL),
		option.WithHTTPClient(server.Client()),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})
	return client
}

func setupShortTestServer(ctx context.Context, t *testing.T) *storage.Client {
	t.Helper()
	downloadTestFiles(t)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		http.ServeFile(w, req, filepath.Join("tmp", "testfiles", "short", path.Base(req.URL.Path)))
	}))
	t.Cleanup(func() {
		server.Close()
	})
	client, err := storage.NewClient(ctx,
		option.WithoutAuthentication(),
		option.WithEndpoint(server.URL),
		option.WithHTTPClient(server.Client()),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})
	return client
}
