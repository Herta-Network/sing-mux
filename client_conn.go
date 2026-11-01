package mux

import (
	"encoding/binary"
	"io"
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
	conn            N.ExtendedConn
	access          sync.Mutex
	packetWrites    packetWriteState
	destination     M.Socksaddr
	requestWritten  bool
	responseRead    bool
	readWaitOptions N.ReadWaitOptions
}

func (c *clientPacketConn) NeedHandshake() bool {
	c.access.Lock()
	defer c.access.Unlock()
	return !c.requestWritten
}

func (c *clientPacketConn) readResponse() error {
	response, err := ReadStreamResponse(c.conn)
	if err != nil {
		return err
	}
	if response.Status == statusError {
		return E.New("remote error: ", response.Message)
	}
	return nil
}

func (c *clientPacketConn) Read(b []byte) (n int, err error) {
	if !c.responseRead {
		err = c.readResponse()
		if err != nil {
			return
		}
		c.responseRead = true
	}
	var length uint16
	err = binary.Read(c.conn, binary.BigEndian, &length)
	if err != nil {
		return
	}
	if cap(b) < int(length) {
		return 0, io.ErrShortBuffer
	}
	return io.ReadFull(c.conn, b[:length])
}

func (c *clientPacketConn) Write(p []byte) (int, error) { return c.WriteTo(p, c.destination) }

func (c *clientPacketConn) ReadBuffer(buffer *buf.Buffer) (err error) {
	if !c.responseRead {
		err = c.readResponse()
		if err != nil {
			return
		}
		c.responseRead = true
	}
	var length uint16
	err = binary.Read(c.conn, binary.BigEndian, &length)
	if err != nil {
		return
	}
	_, err = buffer.ReadFullFrom(c.conn, int(length))
	return
}

func (c *clientPacketConn) WriteBuffer(b *buf.Buffer) error { return c.WritePacket(b, c.destination) }

func (c *clientPacketConn) FrontHeadroom() int {
	return 2
}

func (c *clientPacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	if !c.responseRead {
		err = c.readResponse()
		if err != nil {
			return
		}
		c.responseRead = true
	}
	var length uint16
	err = binary.Read(c.conn, binary.BigEndian, &length)
	if err != nil {
		return
	}
	if cap(p) < int(length) {
		return 0, nil, io.ErrShortBuffer
	}
	n, err = io.ReadFull(c.conn, p[:length])
	if err == nil {
		if c.destination.IsFqdn() {
			addr = c.destination
		} else {
			addr = c.destination.UDPAddr()
		}
	}
	return
}

func (c *clientPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	n, err := c.writePacketCount(buf.As(p), M.SocksaddrFromNet(addr))
	if n == 1 {
		return len(p), err
	}
	return 0, err
}

func (c *clientPacketConn) ReadPacket(buffer *buf.Buffer) (destination M.Socksaddr, err error) {
	err = c.ReadBuffer(buffer)
	if err == nil {
		destination = c.destination
	}
	return
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
	conn            N.ExtendedConn
	access          sync.Mutex
	packetWrites    packetWriteState
	destination     M.Socksaddr
	requestWritten  bool
	responseRead    bool
	readWaitOptions N.ReadWaitOptions
}

func (c *clientPacketAddrConn) NeedHandshake() bool {
	c.access.Lock()
	defer c.access.Unlock()
	return !c.requestWritten
}

func (c *clientPacketAddrConn) readResponse() error {
	response, err := ReadStreamResponse(c.conn)
	if err != nil {
		return err
	}
	if response.Status == statusError {
		return E.New("remote error: ", response.Message)
	}
	return nil
}

func (c *clientPacketAddrConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	if !c.responseRead {
		err = c.readResponse()
		if err != nil {
			return
		}
		c.responseRead = true
	}
	destination, err := M.SocksaddrSerializer.ReadAddrPort(c.conn)
	if err != nil {
		return
	}
	if destination.IsFqdn() {
		addr = destination
	} else {
		addr = destination.UDPAddr()
	}
	var length uint16
	err = binary.Read(c.conn, binary.BigEndian, &length)
	if err != nil {
		return
	}
	if cap(p) < int(length) {
		return 0, nil, io.ErrShortBuffer
	}
	n, err = io.ReadFull(c.conn, p[:length])
	return
}

func (c *clientPacketAddrConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	n, err := c.writePacketCount(buf.As(p), M.SocksaddrFromNet(addr))
	if n == 1 {
		return len(p), err
	}
	return 0, err
}

func (c *clientPacketAddrConn) ReadPacket(buffer *buf.Buffer) (destination M.Socksaddr, err error) {
	if !c.responseRead {
		err = c.readResponse()
		if err != nil {
			return
		}
		c.responseRead = true
	}
	destination, err = M.SocksaddrSerializer.ReadAddrPort(c.conn)
	if err != nil {
		return
	}
	var length uint16
	err = binary.Read(c.conn, binary.BigEndian, &length)
	if err != nil {
		return
	}
	_, err = buffer.ReadFullFrom(c.conn, int(length))
	return
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
