package mux

import (
	"net"
	"sync"

	"github.com/metacubex/sing/common"
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
	"github.com/metacubex/sing/common/rw"
)

type serverConn struct {
	N.ExtendedConn
	responseWritten bool
}

func (c *serverConn) NeedHandshake() bool {
	return !c.responseWritten
}

func (c *serverConn) HandshakeFailure(err error) error {
	errMessage := err.Error()
	buffer := buf.NewSize(1 + rw.UvarintLen(uint64(len(errMessage))) + len(errMessage))
	defer buffer.Release()
	common.Must(
		buffer.WriteByte(statusError),
		rw.WriteVString(buffer, errMessage),
	)
	return common.Error(c.ExtendedConn.Write(buffer.Bytes()))
}

func (c *serverConn) Write(b []byte) (n int, err error) {
	if c.responseWritten {
		return c.ExtendedConn.Write(b)
	}
	buffer := buf.NewSize(1 + len(b))
	defer buffer.Release()
	common.Must(
		buffer.WriteByte(statusSuccess),
		common.Error(buffer.Write(b)),
	)
	_, err = c.ExtendedConn.Write(buffer.Bytes())
	if err != nil {
		return
	}
	c.responseWritten = true
	return len(b), nil
}

func (c *serverConn) WriteBuffer(buffer *buf.Buffer) error {
	if c.responseWritten {
		return c.ExtendedConn.WriteBuffer(buffer)
	}
	buffer.ExtendHeader(1)[0] = statusSuccess
	c.responseWritten = true
	return c.ExtendedConn.WriteBuffer(buffer)
}

func (c *serverConn) FrontHeadroom() int {
	if !c.responseWritten {
		return 1
	}
	return 0
}

func (c *serverConn) NeedAdditionalReadDeadline() bool {
	return true
}

func (c *serverConn) Upstream() any {
	return c.ExtendedConn
}

type serverPacketConn struct {
	N.ExtendedConn
	access          sync.Mutex
	packetWrites    packetWriteState
	packetReads     packetReadState
	destination     M.Socksaddr
	responseWritten bool
}

func (c *serverPacketConn) NeedHandshake() bool {
	c.access.Lock()
	defer c.access.Unlock()
	return !c.responseWritten
}

func (c *serverPacketConn) HandshakeFailure(err error) error {
	errMessage := err.Error()
	buffer := buf.NewSize(1 + rw.UvarintLen(uint64(len(errMessage))) + len(errMessage))
	defer buffer.Release()
	common.Must(
		buffer.WriteByte(statusError),
		rw.WriteVString(buffer, errMessage),
	)
	return common.Error(c.ExtendedConn.Write(buffer.Bytes()))
}

func (c *serverPacketConn) ReadPacket(buffer *buf.Buffer) (M.Socksaddr, error) {
	return c.packetReads.readBuffer(c.ExtendedConn, false, c.destination, nil, buffer)
}

func (c *serverPacketConn) WritePacket(b *buf.Buffer, d M.Socksaddr) error {
	_, err := c.writePacketCount(b, d)
	return err
}

func (c *serverPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	n, destination, err := c.packetReads.read(c.ExtendedConn, false, c.destination, nil, p)
	if err != nil {
		return n, nil, err
	}
	if destination.IsFqdn() {
		return n, destination, nil
	}
	return n, destination.UDPAddr(), nil
}

func (c *serverPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	n, err := c.writePacketCount(buf.As(p), M.SocksaddrFromNet(addr))
	if n == 1 {
		return len(p), err
	}
	return 0, err
}

func (c *serverPacketConn) NeedAdditionalReadDeadline() bool {
	return true
}

func (c *serverPacketConn) Upstream() any {
	return c.ExtendedConn
}

func (c *serverPacketConn) FrontHeadroom() int {
	return 3
}

type serverPacketAddrConn struct {
	N.ExtendedConn
	access          sync.Mutex
	packetWrites    packetWriteState
	packetReads     packetReadState
	responseWritten bool
}

func (c *serverPacketAddrConn) NeedHandshake() bool {
	c.access.Lock()
	defer c.access.Unlock()
	return !c.responseWritten
}

func (c *serverPacketAddrConn) HandshakeFailure(err error) error {
	errMessage := err.Error()
	buffer := buf.NewSize(1 + rw.UvarintLen(uint64(len(errMessage))) + len(errMessage))
	defer buffer.Release()
	common.Must(
		buffer.WriteByte(statusError),
		rw.WriteVString(buffer, errMessage),
	)
	return common.Error(c.ExtendedConn.Write(buffer.Bytes()))
}

func (c *serverPacketAddrConn) ReadFrom(p []byte) (int, net.Addr, error) {
	n, destination, err := c.packetReads.read(c.ExtendedConn, true, M.Socksaddr{}, nil, p)
	if err != nil {
		return n, nil, err
	}
	if destination.IsFqdn() {
		return n, destination, nil
	}
	return n, destination.UDPAddr(), nil
}

func (c *serverPacketAddrConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	n, err := c.writePacketCount(buf.As(p), M.SocksaddrFromNet(addr))
	if n == 1 {
		return len(p), err
	}
	return 0, err
}

func (c *serverPacketAddrConn) ReadPacket(buffer *buf.Buffer) (M.Socksaddr, error) {
	return c.packetReads.readBuffer(c.ExtendedConn, true, M.Socksaddr{}, nil, buffer)
}

func (c *serverPacketAddrConn) WritePacket(b *buf.Buffer, d M.Socksaddr) error {
	_, err := c.writePacketCount(b, d)
	return err
}

func (c *serverPacketAddrConn) NeedAdditionalReadDeadline() bool {
	return true
}

func (c *serverPacketAddrConn) Upstream() any {
	return c.ExtendedConn
}

func (c *serverPacketAddrConn) FrontHeadroom() int {
	return 3 + M.MaxSocksaddrLength
}
