package gharchive

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_concurrentScanner(t *testing.T) {
	t.Run("short", func(t *testing.T) {
		ctx := context.Background()
		client := setupShortTestServer(ctx, t)
		start := time.Date(2020, 10, 10, 8, 6, 0, 0, time.UTC)
		opts := &Options{
			StorageClient: client,
			Concurrency:   3,
			EndTime:       start.Add(150 * time.Minute),
		}
		scanner, err := newConcurrentScanner(ctx, start, opts)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, scanner.Close())
		})
		var count int
		for scanner.Scan(ctx) {
			count++
		}
		require.NoError(t, scanner.Err())
		require.Equal(t, 33, count)
	})

	t.Run("regular", func(t *testing.T) {
		if testing.Short() {
			t.SkipNow()
		}
		ctx := context.Background()
		client := setupTestServer(ctx, t)
		start := time.Date(2020, 10, 10, 8, 6, 0, 0, time.UTC)
		opts := &Options{
			StorageClient: client,
			Concurrency:   3,
			EndTime:       start.Add(159 * time.Minute),
		}
		scanner, err := newConcurrentScanner(ctx, start, opts)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, scanner.Close())
		})
		var count int
		for scanner.Scan(ctx) {
			count++
		}
		require.NoError(t, scanner.Err())
		require.Equal(t, 280628, count)
	})
}
