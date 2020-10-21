package gharchive

import "bytes"

type lineScanner struct {
	br  byteReader
	pos int
}

func (s *lineScanner) scan() bool {
	s.br.release(s.pos)
	for {
		idx := bytes.IndexByte(s.br.window(), '\n')
		if idx >= 0 {
			s.pos = idx + 1
			return true
		}
		if s.br.extend() == 0 {
			s.pos = len(s.br.window())
			return s.pos > 0
		}
	}
}

func (s *lineScanner) bytes() []byte {
	return s.br.window()[:s.pos]
}

func (s *lineScanner) error() error {
	if len(s.br.window()) > 0 {
		return nil
	}
	return s.br.err
}
