package mux

import (
	"net"
	"sync"

	"github.com/metacubex/sing/common/buf"
	E "github.com/metacubex/sing/common/exceptions"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

type clientConn struct {
	net.Conn
	destination    M.Socksaddr
	requestWritten bool
	responseRead   bool
}

func (c *clientConn) NeedHandshake() bool {
	return !c.requestWritten
}

func (c *clientConn) readResponse() error {
	response, err := ReadStreamResponse(c.Conn)
	if err != nil {
		return err
	}
	if response.Status == statusError {
		return E.New("remote error: ", response.Message)
	}
	return nil
}

func (c *clientConn) Read(b []byte) (n int, err error) {
	if !c.responseRead {
		err = c.readResponse()
		if err != nil {
			return
		}
		c.responseRead = true
	}
	return c.Conn.Read(b)
}

func (c *clientConn) Write(b []byte) (n int, err error) {
	if c.requestWritten {
		return c.Conn.Write(b)
	}
	request := StreamRequest{
		Network:     N.NetworkTCP,
		Destination: c.destination,
	}
	buffer := buf.NewSize(streamRequestLen(request) + len(b))
	defer buffer.Release()
	err = EncodeStreamRequest(request, buffer)
	if err != nil {
		return
	}
	buffer.Write(b)
	_, err = c.Conn.Write(buffer.Bytes())
	if err != nil {
		return
	}
	c.requestWritten = true
	return len(b), nil
}

func (c *clientConn) LocalAddr() net.Addr {
	return c.Conn.LocalAddr()
}

func (c *clientConn) RemoteAddr() net.Addr {
	return c.destination.TCPAddr()
}

func (c *clientConn) ReaderReplaceable() bool {
	return c.responseRead
}

func (c *clientConn) WriterReplaceable() bool {
	return c.requestWritten
}

func (c *clientConn) NeedAdditionalReadDeadline() bool {
	return true
}

func (c *clientConn) Upstream() any {
	return c.Conn
}

type clientConnWithCloseWrite struct {
	*clientConn
}

func (c *clientConnWithCloseWrite) CloseWrite() error {
	return N.CloseWrite(c.Conn)
}

var _ N.NetPacketConn = (*clientPacketConn)(nil)

type clientPacketConn struct {
	N.AbstractConn
	conn           N.ExtendedConn
	access         sync.Mutex
	packetWrites   packetWriteState
	packetReads    packetReadState
	destination    M.Socksaddr
	requestWritten bool
	responseRead   bool
}

func (c *clientPacketConn) NeedHandshake() bool {
	c.access.Lock()
	defer c.access.Unlock()
	return !c.requestWritten
}

func (c *clientPacketConn) Read(p []byte) (int, error) {
	n, _, err := c.packetReads.read(c.conn, false, c.destination, &c.responseRead, p)
	return n, err
}

func (c *clientPacketConn) Write(p []byte) (int, error) { return c.WriteTo(p, c.destination) }

func (c *clientPacketConn) ReadBuffer(buffer *buf.Buffer) error {
	_, err := c.ReadPacket(buffer)
	return err
}

func (c *clientPacketConn) WriteBuffer(b *buf.Buffer) error { return c.WritePacket(b, c.destination) }

func (c *clientPacketConn) FrontHeadroom() int {
	return 2
}

func (c *clientPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	n, destination, err := c.packetReads.read(c.conn, false, c.destination, &c.responseRead, p)
	if err != nil {
		return n, nil, err
	}
	if destination.IsFqdn() {
		return n, destination, nil
	}
	return n, destination.UDPAddr(), nil
}

func (c *clientPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	n, err := c.writePacketCount(buf.As(p), M.SocksaddrFromNet(addr))
	if n == 1 {
		return len(p), err
	}
	return 0, err
}

func (c *clientPacketConn) ReadPacket(buffer *buf.Buffer) (M.Socksaddr, error) {
	return c.packetReads.readBuffer(c.conn, false, c.destination, &c.responseRead, buffer)
}

func (c *clientPacketConn) WritePacket(b *buf.Buffer, d M.Socksaddr) error {
	_, err := c.writePacketCount(b, d)
	return err
}

func (c *clientPacketConn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *clientPacketConn) RemoteAddr() net.Addr {
	return c.destination.UDPAddr()
}

func (c *clientPacketConn) NeedAdditionalReadDeadline() bool {
	return true
}

func (c *clientPacketConn) Upstream() any {
	return c.conn
}

var _ N.NetPacketConn = (*clientPacketAddrConn)(nil)

type clientPacketAddrConn struct {
	N.AbstractConn
	conn           N.ExtendedConn
	access         sync.Mutex
	packetWrites   packetWriteState
	packetReads    packetReadState
	destination    M.Socksaddr
	requestWritten bool
	responseRead   bool
}

func (c *clientPacketAddrConn) NeedHandshake() bool {
	c.access.Lock()
	defer c.access.Unlock()
	return !c.requestWritten
}

func (c *clientPacketAddrConn) ReadFrom(p []byte) (int, net.Addr, error) {
	n, destination, err := c.packetReads.read(c.conn, true, M.Socksaddr{}, &c.responseRead, p)
	if err != nil {
		return n, nil, err
	}
	if destination.IsFqdn() {
		return n, destination, nil
	}
	return n, destination.UDPAddr(), nil
}

func (c *clientPacketAddrConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	n, err := c.writePacketCount(buf.As(p), M.SocksaddrFromNet(addr))
	if n == 1 {
		return len(p), err
	}
	return 0, err
}

func (c *clientPacketAddrConn) ReadPacket(buffer *buf.Buffer) (M.Socksaddr, error) {
	return c.packetReads.readBuffer(c.conn, true, M.Socksaddr{}, &c.responseRead, buffer)
}

func (c *clientPacketAddrConn) WritePacket(b *buf.Buffer, d M.Socksaddr) error {
	_, err := c.writePacketCount(b, d)
	return err
}

func (c *clientPacketAddrConn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *clientPacketAddrConn) FrontHeadroom() int {
	return 2 + M.MaxSocksaddrLength
}

func (c *clientPacketAddrConn) NeedAdditionalReadDeadline() bool {
	return true
}

func (c *clientPacketAddrConn) Upstream() any {
	return c.conn
}
