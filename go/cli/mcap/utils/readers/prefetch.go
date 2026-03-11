package readers

import (
	"io"
)

// rangeReader is implemented by readers that support closed-range GETs.
// Using bytes=X-Y (vs bytes=X-) allows the HTTP connection to be reused
// after the response body is fully drained — no new TLS handshake per fill.
type rangeReader interface {
	ReadRange(start, end int64) (io.ReadCloser, error)
}

// bufReadSize controls how many bytes we pull per fill. Larger = fewer HTTP
// requests on sequential access patterns (good). Larger also means more
// wasted bytes when a Seek() lands outside the current window (bad for
// fully random access). 32 MB is a reasonable balance: MCAP chunks are
// typically 0.5–5 MB, so a 32 MB window covers 6–60 chunks without
// overfetching too aggressively on seeks.
const bufReadSize = 64 * 1024 * 1024 // 64 MB

// BufferedReadSeekCloser wraps any io.ReadSeekCloser with an eager
// read-ahead buffer. On each buffer miss it issues a single large range
// request, reducing the number of HTTP round trips on sequential or
// near-sequential workloads (e.g. reading adjacent MCAP chunks).
//
// Seeks within the current window are free (no network). Seeks outside
// the window invalidate the buffer; the next Read re-fills from the new
// position. Single-threaded — no goroutines, no concurrent access to inner.
type BufferedReadSeekCloser struct {
	inner    io.ReadSeekCloser
	size     int64
	offset   int64 // logical read cursor
	buf      []byte
	bufStart int64 // file offset of buf[0]
	bufEnd   int64 // file offset one past last valid byte
}

// NewBufferedReadSeekCloser wraps rs with a 32 MB read-ahead buffer.
// size must equal the total object size (from HeadObject).
func NewBufferedReadSeekCloser(rs io.ReadSeekCloser, size int64) *BufferedReadSeekCloser {
	return &BufferedReadSeekCloser{inner: rs, size: size}
}

// fill fetches [p.offset, p.offset+bufReadSize) from the inner reader.
// If the inner reader implements rangeReader, it issues a closed-range GET
// (bytes=X-Y) so the TCP connection can be reused after the body is drained.
// Otherwise it falls back to Seek + ReadFull on the open-ended stream.
func (p *BufferedReadSeekCloser) fill() error {
	end := p.offset + bufReadSize
	if end > p.size {
		end = p.size
	}
	length := end - p.offset
	if length <= 0 {
		return io.EOF
	}

	buf := make([]byte, length)

	if rr, ok := p.inner.(rangeReader); ok {
		// Closed-range path (bytes=X-Y): response body is exactly `length`
		// bytes → io.ReadFull drains it fully → TCP connection returned to
		// pool, no new TLS handshake needed for the next fill.
		rc, err := rr.ReadRange(p.offset, end)
		if err != nil {
			return err
		}
		n, err := io.ReadFull(rc, buf)
		rc.Close()
		if err != nil && err != io.ErrUnexpectedEOF {
			return err
		}
		buf = buf[:n]
	} else {
		// Fallback: open-ended Seek + ReadFull (drops connection after fill).
		if _, err := p.inner.Seek(p.offset, io.SeekStart); err != nil {
			return err
		}
		n, err := io.ReadFull(p.inner, buf)
		if err != nil && err != io.ErrUnexpectedEOF {
			return err
		}
		buf = buf[:n]
	}

	p.buf = buf
	p.bufStart = p.offset
	p.bufEnd = p.offset + int64(len(buf))
	return nil
}

// Read implements io.Reader.
func (p *BufferedReadSeekCloser) Read(dst []byte) (int, error) {
	if p.offset >= p.size {
		return 0, io.EOF
	}
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

// Seek implements io.Seeker. Seeks within the buffer are free.
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
