package readers

import (
	"io"
)

// bufReadSize is how many bytes we pull from the network in one shot.
// Larger = fewer HTTP round trips = better throughput on high-latency links.
// MCAP chunks are 1–6 MB; 64 MB covers ~10–60 chunks per fetch.
const bufReadSize = 64 * 1024 * 1024 // 64 MB

// BufferedReadSeekCloser wraps any io.ReadSeekCloser with a large eager
// read-ahead buffer. On every buffer miss it issues a single large range
// request instead of many small ones, dramatically reducing round trips on
// high-latency S3-compatible storage.
//
// Design: single-threaded, no goroutines, so there is never concurrent access
// to the underlying reader. Seek() within the current buffer window is free
// (no network). Seek() outside the window re-fills the buffer from the new
// position.
type BufferedReadSeekCloser struct {
	inner    io.ReadSeekCloser
	size     int64 // total object size
	offset   int64 // current logical read position
	buf      []byte
	bufStart int64 // file offset where buf[0] lives
	bufEnd   int64 // file offset one past the last valid byte in buf
}

// NewBufferedReadSeekCloser wraps rs with a 64 MB eager read buffer.
// size is the total object size returned from HeadObject.
func NewBufferedReadSeekCloser(rs io.ReadSeekCloser, size int64) *BufferedReadSeekCloser {
	return &BufferedReadSeekCloser{
		inner: rs,
		size:  size,
	}
}

// fill fetches up to bufReadSize bytes starting at p.offset from the inner reader.
func (p *BufferedReadSeekCloser) fill() error {
	end := p.offset + bufReadSize
	if end > p.size {
		end = p.size
	}
	length := end - p.offset
	if length <= 0 {
		return io.EOF
	}

	if _, err := p.inner.Seek(p.offset, io.SeekStart); err != nil {
		return err
	}

	buf := make([]byte, length)
	n, err := io.ReadFull(p.inner, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		return err
	}

	p.buf = buf[:n]
	p.bufStart = p.offset
	p.bufEnd = p.offset + int64(n)
	return nil
}

// Read implements io.Reader.
func (p *BufferedReadSeekCloser) Read(dst []byte) (int, error) {
	if p.offset >= p.size {
		return 0, io.EOF
	}

	// Refill if offset is outside current buffer.
	if p.buf == nil || p.offset < p.bufStart || p.offset >= p.bufEnd {
		if err := p.fill(); err != nil {
			return 0, err
		}
	}

	pos := int(p.offset - p.bufStart)
	n := copy(dst, p.buf[pos:])
	p.offset += int64(n)
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

// Seek implements io.Seeker. Seeks within the buffer are free (no network I/O).
func (p *BufferedReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	var target int64
	switch whence {
	case io.SeekStart:
		target = offset
	case io.SeekCurrent:
		target = p.offset + offset
	case io.SeekEnd:
		target = p.size + offset
	default:
		return 0, io.ErrNoProgress
	}
	if target < 0 || target > p.size {
		return 0, io.ErrNoProgress
	}
	p.offset = target
	return target, nil
}

// Close implements io.Closer.
func (p *BufferedReadSeekCloser) Close() error {
	p.buf = nil
	return p.inner.Close()
}
