package gharchive

import (
	"bytes"
	"context"
	"io"
	"sync"
	"time"

	"github.com/killa-beez/gopkgs/pool"
)

type concurrentScanner struct {
	scanners    []*singleScanner
	scannerErrs []error
	lines       chan []byte
	cancel      func()
	bytes       []byte

	errLock sync.RWMutex
	err     error

	doneLock sync.Mutex
	doneChan chan struct{}
	done     bool
}

func newConcurrentScanner(ctx context.Context, startTime time.Time, opts *Options) (*concurrentScanner, error) {
	if opts == nil {
		opts = new(Options)
	}
	endTime := opts.EndTime
	if endTime.IsZero() {
		endTime = startTime.Add(time.Hour)
	}
	opts.SingleHour = true
	startTime = startTime.UTC()
	hour := time.Date(startTime.Year(), startTime.Month(), startTime.Day(), startTime.Hour(), 0, 0, 0, time.UTC)
	var scanners []*singleScanner
	for hour.Before(endTime) {
		scanner, err := newSingleScanner(ctx, hour, opts)
		if err != nil {
			return nil, err
		}
		scanners = append(scanners, scanner)
		hour = hour.Add(time.Hour)
	}
	m := &concurrentScanner{
		scanners:    scanners,
		scannerErrs: make([]error, len(scanners)),
		lines:       make(chan []byte, opts.Concurrency*100_000),
		doneChan:    make(chan struct{}),
	}
	ctx, m.cancel = context.WithCancel(ctx)

	p := pool.New(len(scanners), opts.Concurrency)
	for i := range scanners {
		i := i
		scanner := scanners[i]
		p.Add(pool.NewWorkUnit(func(ctx2 context.Context) {
			scannerErr := runScanner(ctx2, scanner, m.lines)
			if scannerErr == io.EOF {
				scannerErr = nil
			}
			m.scannerErrs[i] = scannerErr
		}))
	}
	p.Start(ctx)
	go func() {
		p.Wait()
		m.beDone()
	}()
	return m, nil
}

func (m *concurrentScanner) beDone() {
	m.doneLock.Lock()
	defer m.doneLock.Unlock()
	if m.done {
		return
	}
	close(m.doneChan)
	m.done = true
}

var bufPool sync.Pool

func runScanner(ctx context.Context, scanner *singleScanner, lines chan<- []byte) error {
	buf, ok := bufPool.Get().(*bytes.Buffer)
	if !ok {
		buf = bytes.NewBuffer(make([]byte, 0, 8192))
	}
	defer bufPool.Put(buf)
	for scanner.Scan(ctx) {
		buf.Reset()
		_, err := buf.ReadFrom(bytes.NewReader(scanner.Bytes()))
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case lines <- buf.Bytes():
		}
	}
	return scanner.Err()
}

func (m *concurrentScanner) Close() error {
	m.cancel()
	var err error
	for _, scanner := range m.scanners {
		closeErr := scanner.Close()
		if err == nil {
			err = closeErr
		}
	}
	m.beDone()
	return err
}

func (m *concurrentScanner) Err() error {
	m.errLock.RLock()
	err := m.err
	m.errLock.RUnlock()
	return err
}

func (m *concurrentScanner) Scan(_ context.Context) bool {
	select {
	case m.bytes = <-m.lines:
		return true
	default:
	}

	select {
	case m.bytes = <-m.lines:
		return true
	case <-m.doneChan:
		m.errLock.Lock()
		for _, err := range m.scannerErrs {
			if err != nil {
				m.err = err
				break
			}
		}
		m.errLock.Unlock()
		return false
	}
}

func (m *concurrentScanner) Bytes() []byte {
	return m.bytes
}
