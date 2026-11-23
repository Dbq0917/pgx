package pgx

import (
	"bufio"
	"bytes"
	"io"
)

// lineScanner reads newline-delimited lines without bufio.Scanner's fixed
// token size limit. Supplier feeds can contain arbitrarily long records, so
// lines are accumulated across internal buffer boundaries.
type lineScanner struct {
	r   *bufio.Reader
	buf []byte
	err error
}

func newLineScanner(r io.Reader) *lineScanner {
	return &lineScanner{r: bufio.NewReaderSize(r, 64*1024)}
}

// Scan advances to the next line, returning false at end of input or error.
func (s *lineScanner) Scan() bool {
	s.buf = s.buf[:0]

	for {
		chunk, err := s.r.ReadSlice('\n')
		s.buf = append(s.buf, chunk...)
		if err == nil {
			break
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		if err == io.EOF {
			// A final line without a trailing newline is still a line.
			s.err = nil
			return len(s.buf) > 0
		}
		s.err = err
		return false
	}

	s.buf = trimLineEnd(s.buf)
	return true
}

// Bytes returns the current line without its trailing line ending. The
// returned slice is reused on the next call and must be copied if retained.
func (s *lineScanner) Bytes() []byte {
	return s.buf
}

// Err returns the first non-EOF read error.
func (s *lineScanner) Err() error {
	return s.err
}

func trimLineEnd(line []byte) []byte {
	line = bytes.TrimSuffix(line, []byte{'\n'})
	line = bytes.TrimSuffix(line, []byte{'\r'})
	return line
}
