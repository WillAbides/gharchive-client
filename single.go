package gharchive

import (
	"context"
	"io"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/klauspost/compress/gzip"
)

// singleScanner scans lines from gharchive
type singleScanner struct {
	opts        *Options
	client      *storage.Client
	bucket      string
	startTime   time.Time
	endTime     time.Time
	curHour     time.Time
	lineScanner *lineScanner
	hourReader  *objReader
	brBuffer    []byte
	err         error
}

func newSingleScanner(ctx context.Context, startTime time.Time, opts *Options) (*singleScanner, error) {
	var err error
	opts, err = opts.withDefaults(ctx)
	if err != nil {
		return nil, err
	}

	endTime := opts.EndTime
	if endTime.IsZero() {
		endTime = startTime.Add(time.Hour)
	}
	return &singleScanner{
		opts:      opts,
		bucket:    opts.Bucket,
		client:    opts.StorageClient,
		startTime: startTime.UTC(),
		endTime:   endTime.UTC(),
	}, nil
}

func (s *singleScanner) iterateCurHour() {
	if s.curHour.IsZero() {
		s.curHour = s.startTime.Truncate(time.Hour)
		return
	}
	s.curHour = s.curHour.Add(time.Hour)
}

// Close closes the scanner
func (s *singleScanner) Close() error {
	var err error
	if s.client != nil {
		err = s.client.Close()
	}
	if s.hourReader != nil {
		hrErr := s.hourReader.Close()
		if err == nil {
			err = hrErr
		}
	}
	return err
}

func (s *singleScanner) validateLine(line []byte) bool {
	for _, validator := range s.opts.Validators {
		ok := validator(line)
		if !ok {
			return false
		}
	}
	return true
}

func (s *singleScanner) prepLineScanner(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if len(s.brBuffer) == 0 {
		s.brBuffer = make([]byte, 8192)
	}
	if s.lineScanner != nil {
		err := s.lineScanner.error()
		if s.opts.SingleHour || err != io.EOF {
			return err
		}
	}

	// starting here we know either s.lineScanner == nil or s.lineScanner.error() == io.EOF
	// either way we need to do the same thing.

	if s.hourReader == nil {
		s.hourReader = new(objReader)
	}
	s.iterateCurHour()
	if s.curHour.After(s.endTime) {
		return io.EOF
	}
	err := s.hourReader.newObj(ctx, s.curHour, s.opts)
	if err != nil {
		return err
	}
	s.lineScanner = &lineScanner{
		br: byteReader{
			data: s.brBuffer,
			r:    s.hourReader,
		},
	}
	return nil
}

// Bytes returns the current line
func (s *singleScanner) Bytes() []byte {
	return s.lineScanner.bytes()
}

// Err returns the scanner's error
func (s *singleScanner) Err() error {
	err := s.err
	if err == io.EOF {
		err = nil
	}
	return err
}

// Scan advances to the next line
func (s *singleScanner) Scan(ctx context.Context) bool {
	for {
		if ctx.Err() != nil {
			s.err = ctx.Err()
			return false
		}
		err := s.prepLineScanner(ctx)
		if err != nil {
			s.err = err
			return false
		}
		s.lineScanner.scan()
		if s.validateLine(s.lineScanner.bytes()) {
			return true
		}
	}
}

type objReader struct {
	rdr   io.Reader
	gzRdr *gzip.Reader
}

func (z *objReader) Read(p []byte) (n int, err error) {
	return z.gzRdr.Read(p)
}

func (z *objReader) Close() error {
	var err error
	if z.gzRdr != nil {
		err = z.gzRdr.Close()
	}
	if z.rdr == nil {
		return err
	}
	if rdr, ok := z.rdr.(io.Closer); ok {
		rdrErr := rdr.Close()
		if rdrErr != nil {
			return rdrErr
		}
	}
	return err
}

func (z *objReader) Reset(r io.Reader) error {
	err := z.Close()
	if err != nil {
		return err
	}
	z.rdr = r
	if z.gzRdr == nil {
		z.gzRdr, err = gzip.NewReader(r)
		return err
	}
	return z.gzRdr.Reset(r)
}

func (z *objReader) newObj(ctx context.Context, hour time.Time, opts *Options) error {
	var err error
	tm := hour.UTC()

	// this hack is required to get a single-digit hour in the object name
	obj := tm.UTC().Format("2006-01-02-")
	obj += strings.TrimPrefix(tm.UTC().Format("15.json.gz"), "0")

	rdr, err := opts.StorageClient.Bucket(opts.Bucket).Object(obj).NewReader(ctx)
	if err != nil {
		return err
	}
	return z.Reset(rdr)
}
