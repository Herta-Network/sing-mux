package mux

import (
	"encoding/binary"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"

	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

const packetBatchLimit = 64

type packetWriteState struct {
	err      error // protected by the connection's access mutex
	closed   atomic.Bool
	once     sync.Once
	closeErr error
}

func (s *packetWriteState) close(conn io.Closer) error {
	s.closed.Store(true)
	s.once.Do(func() { s.closeErr = conn.Close() })
	return s.closeErr
}
func (s *packetWriteState) check() error {
	if s.err != nil {
		return s.err
	}
	if s.closed.Load() {
		return net.ErrClosed
	}
	return nil
}
func validPacketAddress(addr M.Socksaddr) bool {
	return addr.IsIP() || (addr.IsFqdn() && len(addr.Fqdn) <= 255)
}

type packetPrepare func(*buf.Buffer, M.Socksaddr) (*buf.Buffer, error)

func (s *packetWriteState) write(access *sync.Mutex, conn N.ExtendedConn, buffers []*buf.Buffer, destinations []M.Socksaddr, bound *M.Socksaddr, prepare packetPrepare) (written int, err error) {
	access.Lock()
	defer access.Unlock()
	return s.writeLocked(conn, buffers, destinations, bound, prepare)
}

func (s *packetWriteState) writeOne(access *sync.Mutex, conn N.ExtendedConn, buffer *buf.Buffer, destination M.Socksaddr, bound *M.Socksaddr, prepare packetPrepare) (int, error) {
	access.Lock()
	defer access.Unlock()
	return s.writeLocked(conn, []*buf.Buffer{buffer}, []M.Socksaddr{destination}, bound, prepare)
}

func (s *packetWriteState) writeLocked(conn N.ExtendedConn, buffers []*buf.Buffer, destinations []M.Socksaddr, bound *M.Socksaddr, prepare packetPrepare) (written int, err error) {
	if err = s.check(); err != nil {
		buf.ReleaseMulti(buffers)
		return
	}
	if len(buffers) == 0 || len(buffers) != len(destinations) {
		buf.ReleaseMulti(buffers)
		return 0, os.ErrInvalid
	}
	for i, b := range buffers {
		if b == nil || b.Len() > 65535 || !validPacketAddress(destinations[i]) {
			buf.ReleaseMulti(buffers)
			return 0, os.ErrInvalid
		}
		if bound != nil && destinations[i].Unwrap() != bound.Unwrap() {
			buf.ReleaseMulti(buffers)
			return 0, os.ErrInvalid
		}
	}
	writer := bufio.NewVectorisedWriterWithCount(conn)
	for start := 0; start < len(buffers); {
		if err = s.check(); err != nil {
			buf.ReleaseMulti(buffers[start:])
			return
		}
		end := start + packetBatchLimit
		if end > len(buffers) {
			end = len(buffers)
		}
		var ends [packetBatchLimit]int64
		var total int64
		for i := start; i < end; i++ {
			prepared, encodeErr := prepare(buffers[i], destinations[i])
			if prepared != nil {
				buffers[i] = prepared
			}
			if encodeErr != nil {
				buf.ReleaseMulti(buffers[start:])
				s.err = encodeErr
				_ = s.close(conn)
				return written, encodeErr
			}
			total += int64(buffers[i].Len())
			ends[i-start] = total
		}
		n, writeErr := writer.WriteVectorisedCount(buffers[start:end])
		if n < 0 || n > total {
			n, writeErr = 0, io.ErrShortWrite
		} else if n != total && writeErr == nil {
			writeErr = io.ErrShortWrite
		}
		for _, boundary := range ends[:end-start] {
			if boundary > n {
				break
			}
			written++
		}
		if writeErr != nil {
			buf.ReleaseMulti(buffers[end:])
			s.err = writeErr
			_ = s.close(conn)
			return written, writeErr
		}
		start = end
	}
	return
}
func prepareClientPacket(buffer *buf.Buffer, dest M.Socksaddr, request StreamRequest, written *bool) (*buf.Buffer, error) {
	payloadLen := buffer.Len()
	headerLen := 2
	if request.PacketAddr {
		headerLen += M.SocksaddrSerializer.AddrPortLen(dest)
	}
	requestLen := 0
	if !*written {
		requestLen = streamRequestLen(request)
	}
	buffer = (N.ReadWaitOptions{FrontHeadroom: requestLen + headerLen}).Copy(buffer)
	header := buf.With(buffer.ExtendHeader(headerLen))
	if request.PacketAddr {
		if err := M.SocksaddrSerializer.WriteAddrPortBuffer(header, dest); err != nil {
			return buffer, err
		}
	}
	binary.BigEndian.PutUint16(header.Extend(2), uint16(payloadLen))
	if !*written {
		if err := EncodeStreamRequest(request, buf.With(buffer.ExtendHeader(requestLen))); err != nil {
			return buffer, err
		}
		*written = true
	}
	return buffer, nil
}
func prepareServerPacket(buffer *buf.Buffer, dest M.Socksaddr, packetAddr bool, written *bool) (*buf.Buffer, error) {
	payloadLen := buffer.Len()
	headerLen := 2
	if packetAddr {
		headerLen += M.SocksaddrSerializer.AddrPortLen(dest)
	}
	if !*written {
		headerLen++
	}
	buffer = (N.ReadWaitOptions{FrontHeadroom: headerLen}).Copy(buffer)
	header := buf.With(buffer.ExtendHeader(headerLen))
	if !*written {
		header.WriteByte(statusSuccess)
		*written = true
	}
	if packetAddr {
		if err := M.SocksaddrSerializer.WriteAddrPortBuffer(header, dest); err != nil {
			return buffer, err
		}
	}
	binary.BigEndian.PutUint16(header.Extend(2), uint16(payloadLen))
	return buffer, nil
}

func (c *clientPacketConn) WritePacketBatchCount(buffers []*buf.Buffer, destinations []M.Socksaddr) (int, error) {
	return c.packetWrites.write(&c.access, c.conn, buffers, destinations, &c.destination, func(b *buf.Buffer, d M.Socksaddr) (*buf.Buffer, error) {
		return prepareClientPacket(b, d, StreamRequest{Network: N.NetworkUDP, Destination: c.destination}, &c.requestWritten)
	})
}
func (c *clientPacketAddrConn) WritePacketBatchCount(buffers []*buf.Buffer, destinations []M.Socksaddr) (int, error) {
	return c.packetWrites.write(&c.access, c.conn, buffers, destinations, nil, func(b *buf.Buffer, d M.Socksaddr) (*buf.Buffer, error) {
		return prepareClientPacket(b, d, StreamRequest{Network: N.NetworkUDP, Destination: c.destination, PacketAddr: true}, &c.requestWritten)
	})
}
func (c *serverPacketConn) WritePacketBatchCount(buffers []*buf.Buffer, destinations []M.Socksaddr) (int, error) {
	return c.packetWrites.write(&c.access, c.ExtendedConn, buffers, destinations, &c.destination, func(b *buf.Buffer, d M.Socksaddr) (*buf.Buffer, error) {
		return prepareServerPacket(b, d, false, &c.responseWritten)
	})
}
func (c *serverPacketAddrConn) WritePacketBatchCount(buffers []*buf.Buffer, destinations []M.Socksaddr) (int, error) {
	return c.packetWrites.write(&c.access, c.ExtendedConn, buffers, destinations, nil, func(b *buf.Buffer, d M.Socksaddr) (*buf.Buffer, error) {
		return prepareServerPacket(b, d, true, &c.responseWritten)
	})
}
func (c *clientPacketConn) Close() error     { return c.packetWrites.close(c.conn) }
func (c *clientPacketAddrConn) Close() error { return c.packetWrites.close(c.conn) }
func (c *serverPacketConn) Close() error     { return c.packetWrites.close(c.ExtendedConn) }
func (c *serverPacketAddrConn) Close() error { return c.packetWrites.close(c.ExtendedConn) }

func (c *clientPacketConn) writePacketCount(buffer *buf.Buffer, destination M.Socksaddr) (int, error) {
	return c.packetWrites.writeOne(&c.access, c.conn, buffer, destination, &c.destination, func(b *buf.Buffer, d M.Socksaddr) (*buf.Buffer, error) {
		return prepareClientPacket(b, d, StreamRequest{Network: N.NetworkUDP, Destination: c.destination}, &c.requestWritten)
	})
}

func (c *clientPacketAddrConn) writePacketCount(buffer *buf.Buffer, destination M.Socksaddr) (int, error) {
	return c.packetWrites.writeOne(&c.access, c.conn, buffer, destination, nil, func(b *buf.Buffer, d M.Socksaddr) (*buf.Buffer, error) {
		return prepareClientPacket(b, d, StreamRequest{Network: N.NetworkUDP, Destination: c.destination, PacketAddr: true}, &c.requestWritten)
	})
}

func (c *serverPacketConn) writePacketCount(buffer *buf.Buffer, destination M.Socksaddr) (int, error) {
	return c.packetWrites.writeOne(&c.access, c.ExtendedConn, buffer, destination, &c.destination, func(b *buf.Buffer, d M.Socksaddr) (*buf.Buffer, error) {
		return prepareServerPacket(b, d, false, &c.responseWritten)
	})
}

func (c *serverPacketAddrConn) writePacketCount(buffer *buf.Buffer, destination M.Socksaddr) (int, error) {
	return c.packetWrites.writeOne(&c.access, c.ExtendedConn, buffer, destination, nil, func(b *buf.Buffer, d M.Socksaddr) (*buf.Buffer, error) {
		return prepareServerPacket(b, d, true, &c.responseWritten)
	})
}
