package gharchive

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"cloud.google.com/go/storage"
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
	err := os.MkdirAll(dir, 0o700)
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
		resp, err := http.Get("https://data.gharchive.org/" + filename)
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode)
		file, err := os.Create(filepath.Join(dir, filename))
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, file.Close())
			require.NoError(t, resp.Body.Close())
		})
		_, err = io.Copy(file, resp.Body)
		require.NoError(t, err)
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

func TestScanner(t *testing.T) {
	ctx := context.Background()
	client := setupTestServer(ctx, t)
	start := time.Date(2020, 10, 10, 8, 6, 0, 0, time.UTC)
	end := start.Add(159 * time.Minute)
	opts := &Options{
		StorageClient: client,
	}
	scanner, err := New(ctx, start, end, opts)
	require.NoError(t, err)
	var count int
	for {
		_, err = scanner.Next(ctx)
		if err != nil {
			break
		}
		count++
	}
	require.EqualError(t, err, io.EOF.Error())
	fmt.Println(count)
}
