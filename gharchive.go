package gharchive

import (
	"context"
	"io"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/klauspost/compress/gzip"
	"google.golang.org/api/option"
)

// Validator is a function that returns true when a line passes validation
type Validator func(line []byte) bool

// Options are options for a Scanner
type Options struct {
	StorageClient *storage.Client
	Bucket        string
	Validators    []Validator
}

func (o *Options) withDefaults(ctx context.Context) (*Options, error) {
	if o == nil {
		o = new(Options)
	}
	if o.StorageClient != nil && o.Bucket != "" {
		return o, nil
	}
	out := &Options{
		StorageClient: o.StorageClient,
		Bucket:        o.Bucket,
		Validators:    o.Validators,
	}
	var err error
	if out.StorageClient == nil {
		out.StorageClient, err = storage.NewClient(ctx, option.WithoutAuthentication())
		if err != nil {
			return nil, err
		}
	}
	if out.Bucket == "" {
		out.Bucket = "data.gharchive.org"
	}
	return out, nil
}

// New returns a new Scanner
func New(ctx context.Context, startTime, endTime time.Time, opts *Options) (*Scanner, error) {
	var err error
	opts, err = opts.withDefaults(ctx)
	if err != nil {
		return nil, err
	}

	return &Scanner{
		opts:      opts,
		bucket:    opts.Bucket,
		client:    opts.StorageClient,
		startTime: startTime.UTC(),
		endTime:   endTime.UTC(),
	}, nil
}

// Scanner scans lines from gharchive
type Scanner struct {
	opts        *Options
	client      *storage.Client
	bucket      string
	startTime   time.Time
	endTime     time.Time
	curHour     time.Time
	lineScanner *lineScanner
	hourReader  *objReader
	brBuffer    []byte
}

func (s *Scanner) iterateCurHour() {
	if s.curHour.IsZero() {
		s.curHour = s.startTime.Truncate(time.Hour)
		return
	}
	s.curHour = s.curHour.Add(time.Hour)
}

// Close closes the scanner
func (s *Scanner) Close() error {
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

func (s *Scanner) validateLine(line []byte) bool {
	for _, validator := range s.opts.Validators {
		ok := validator(line)
		if !ok {
			return false
		}
	}
	return true
}

func (s *Scanner) prepLineScanner(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if len(s.brBuffer) == 0 {
		s.brBuffer = make([]byte, 8192)
	}
	if s.lineScanner != nil {
		err := s.lineScanner.error()
		if err == nil || err != io.EOF {
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

// Next returns the next line of output. error is io.EOF at the end.
func (s *Scanner) Next(ctx context.Context) ([]byte, error) {
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		err := s.prepLineScanner(ctx)
		if err != nil {
			return nil, err
		}
		line := s.lineScanner.next()
		if s.validateLine(line) {
			return line, nil
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
	opts, err = opts.withDefaults(ctx)
	if err != nil {
		return err
	}
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
