package gharchive

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_singleScanner(t *testing.T) {
	t.Run("short", func(t *testing.T) {
		t.Run("multi-hour", func(t *testing.T) {
			ctx := context.Background()
			client := setupShortTestServer(ctx, t)
			start := time.Date(2020, 10, 10, 8, 6, 0, 0, time.UTC)
			opts := &Options{
				StorageClient: client,
				EndTime:       start.Add(159 * time.Minute),
			}
			scanner, err := New(ctx, start, opts)
			require.NoError(t, err)
			var count int
			for scanner.Scan(ctx) {
				count++
			}
			require.NoError(t, scanner.Err())
			require.Equal(t, 33, count)
		})

		t.Run("single hour", func(t *testing.T) {
			ctx := context.Background()
			client := setupShortTestServer(ctx, t)
			start := time.Date(2020, 10, 10, 10, 6, 0, 0, time.UTC)
			opts := &Options{
				StorageClient: client,
				SingleHour:    true,
			}
			scanner, err := New(ctx, start, opts)
			require.NoError(t, err)
			var got [][]byte
			for scanner.Scan(ctx) {
				got = append(got, scanner.Bytes())
			}
			require.NoError(t, scanner.Err())
			require.Len(t, got, 11)
		})
	})

	t.Run("regular", func(t *testing.T) {
		if testing.Short() {
			t.SkipNow()
		}
		t.Run("multi-hour", func(t *testing.T) {
			ctx := context.Background()
			client := setupTestServer(ctx, t)
			start := time.Date(2020, 10, 10, 8, 6, 0, 0, time.UTC)
			opts := &Options{
				StorageClient: client,
				EndTime:       start.Add(159 * time.Minute),
			}
			scanner, err := New(ctx, start, opts)
			require.NoError(t, err)
			var count int
			for scanner.Scan(ctx) {
				count++
			}
			require.NoError(t, scanner.Err())
			require.Equal(t, 280628, count)
		})

		t.Run("single hour", func(t *testing.T) {
			ctx := context.Background()
			client := setupTestServer(ctx, t)
			start := time.Date(2020, 10, 10, 8, 6, 0, 0, time.UTC)
			opts := &Options{
				StorageClient: client,
				SingleHour:    true,
			}
			scanner, err := New(ctx, start, opts)
			require.NoError(t, err)
			var count int
			for scanner.Scan(ctx) {
				count++
			}
			require.NoError(t, scanner.Err())
			require.Equal(t, 92993, count)
		})
	})
}
