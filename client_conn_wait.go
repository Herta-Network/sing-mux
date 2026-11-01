package mux

import (
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

var _ N.PacketReadWaiter = (*clientPacketConn)(nil)

func (c *clientPacketConn) InitializeReadWaiter(options N.ReadWaitOptions) bool {
	c.packetReads.Lock()
	c.packetReads.options = options
	c.packetReads.Unlock()
	return false
}
func (c *clientPacketConn) WaitReadPacket() (*buf.Buffer, M.Socksaddr, error) {
	return c.packetReads.wait(c.conn, false, c.destination, &c.responseRead)
}

var _ N.PacketReadWaiter = (*clientPacketAddrConn)(nil)

func (c *clientPacketAddrConn) InitializeReadWaiter(options N.ReadWaitOptions) bool {
	c.packetReads.Lock()
	c.packetReads.options = options
	c.packetReads.Unlock()
	return false
}
func (c *clientPacketAddrConn) WaitReadPacket() (*buf.Buffer, M.Socksaddr, error) {
	return c.packetReads.wait(c.conn, true, M.Socksaddr{}, &c.responseRead)
}

var _ N.PacketReadWaiter = (*serverPacketConn)(nil)

func (c *serverPacketConn) InitializeReadWaiter(options N.ReadWaitOptions) bool {
	c.packetReads.Lock()
	c.packetReads.options = options
	c.packetReads.Unlock()
	return false
}
func (c *serverPacketConn) WaitReadPacket() (*buf.Buffer, M.Socksaddr, error) {
	return c.packetReads.wait(c.ExtendedConn, false, c.destination, nil)
}

var _ N.PacketReadWaiter = (*serverPacketAddrConn)(nil)

func (c *serverPacketAddrConn) InitializeReadWaiter(options N.ReadWaitOptions) bool {
	c.packetReads.Lock()
	c.packetReads.options = options
	c.packetReads.Unlock()
	return false
}
func (c *serverPacketAddrConn) WaitReadPacket() (*buf.Buffer, M.Socksaddr, error) {
	return c.packetReads.wait(c.ExtendedConn, true, M.Socksaddr{}, nil)
}
