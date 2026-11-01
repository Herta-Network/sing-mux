package mux

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"testing"

	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

type packetReadFixture struct {
	net.Conn
	reader *bytes.Reader
}

func (c *packetReadFixture) Read(p []byte) (int, error) { return c.reader.Read(p) }

func packetReadConn(data []byte, client, withAddr bool) N.NetPacketConn {
	c := bufio.NewExtendedConn(&packetReadFixture{reader: bytes.NewReader(data)})
	if client {
		if withAddr {
			return &clientPacketAddrConn{AbstractConn: c, conn: c}
		}
		return &clientPacketConn{AbstractConn: c, conn: c, destination: batchTarget}
	}
	if withAddr {
		return &serverPacketAddrConn{ExtendedConn: c}
	}
	return &serverPacketConn{ExtendedConn: c, destination: batchTarget}
}

func packetReadWire(client, withAddr bool, payloads [][]byte) []byte {
	var wire bytes.Buffer
	if client {
		wire.WriteByte(statusSuccess)
	}
	for i, payload := range payloads {
		if withAddr {
			d := batchTarget
			if i%2 == 1 {
				d = M.ParseSocksaddr("example.test:443")
			}
			if err := M.SocksaddrSerializer.WriteAddrPort(&wire, d); err != nil {
				panic(err)
			}
		}
		binary.Write(&wire, binary.BigEndian, uint16(len(payload)))
		wire.Write(payload)
	}
	return wire.Bytes()
}

func TestPacketReadMixedAPIsAndShortBuffer(t *testing.T) {
	for _, client := range []bool{false, true} {
		for _, withAddr := range []bool{false, true} {
			t.Run(fmt.Sprintf("client=%v/address=%v", client, withAddr), func(t *testing.T) {
				payloads := [][]byte{nil, []byte("short"), bytes.Repeat([]byte{7}, 65535), []byte("tail"), []byte("last")}
				c := packetReadConn(packetReadWire(client, withAddr, payloads), client, withAddr)
				w := c.(N.PacketReadWaiter)
				w.InitializeReadWaiter(N.ReadWaitOptions{FrontHeadroom: 300, RearHeadroom: 32, MTU: 1})
				p, d, err := w.WaitReadPacket()
				if err != nil || p.Len() != 0 || d != batchTarget || p.Start() != 300 || p.FreeLen() < 32 {
					t.Fatalf("empty owned packet: %v %v", d, err)
				}
				p.Release()
				short := make([]byte, 1, 100)
				if _, _, err = c.ReadFrom(short); !errors.Is(err, io.ErrShortBuffer) {
					t.Fatalf("short scalar: %v", err)
				}
				p, d, err = w.WaitReadPacket()
				if err != nil || p.Len() != 65535 || !bytes.Equal(p.Bytes(), payloads[2]) || d != batchTarget {
					t.Fatalf("large after short: %v %v", d, err)
				}
				p.Release()
				b := buf.NewSize(2)
				if _, err = c.ReadPacket(b); !errors.Is(err, io.ErrShortBuffer) {
					t.Fatalf("short owned: %v", err)
				}
				b.Release()
				data := make([]byte, 4)
				if n, d, err := c.ReadFrom(data); err != nil || n != 4 || string(data) != "last" || M.SocksaddrFromNet(d) != batchTarget {
					t.Fatalf("last packet: %d %v %v", n, d, err)
				}
				if p, _, err = w.WaitReadPacket(); p != nil || !errors.Is(err, io.EOF) {
					t.Fatalf("EOF: %v %v", p, err)
				}
			})
		}
	}
}

func TestPacketReadTruncatedIsSticky(t *testing.T) {
	for _, client := range []bool{false, true} {
		for _, withAddr := range []bool{false, true} {
			wire := packetReadWire(client, withAddr, [][]byte{[]byte("truncated")})
			c := packetReadConn(wire[:len(wire)-2], client, withAddr)
			w := c.(N.PacketReadWaiter)
			w.InitializeReadWaiter(N.ReadWaitOptions{RearHeadroom: 32})
			p, _, err := w.WaitReadPacket()
			if p != nil || !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("partial owned packet: %v %v", p, err)
			}
			if _, _, next := c.ReadFrom(make([]byte, 100)); next != err {
				t.Fatalf("lost framing error: %v != %v", next, err)
			}
		}
	}
}

type packetTimeoutReader struct {
	data    *bytes.Reader
	partial bool
	failed  bool
}

func (r *packetTimeoutReader) Read(p []byte) (int, error) {
	if !r.failed {
		r.failed = true
		if r.partial {
			n, _ := r.data.Read(p[:1])
			return n, os.ErrDeadlineExceeded
		}
		return 0, os.ErrDeadlineExceeded
	}
	return r.data.Read(p)
}
func TestPacketReadBoundaryTimeoutCanRetry(t *testing.T) {
	for _, client := range []bool{false, true} {
		for _, withAddr := range []bool{false, true} {
			s := &packetReadState{}
			source := &packetTimeoutReader{data: bytes.NewReader(packetReadWire(client, withAddr, [][]byte{[]byte("next")}))}
			var responseRead bool
			var response *bool
			if client {
				response = &responseRead
			}
			if _, _, err := s.read(source, withAddr, batchTarget, response, make([]byte, 8)); !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatal(err)
			}
			got := make([]byte, 8)
			if n, d, err := s.read(source, withAddr, batchTarget, response, got); err != nil || n != 4 || string(got[:n]) != "next" || d != batchTarget {
				t.Fatalf("timeout poisoned packet boundary: %d %v %v", n, d, err)
			}
		}
	}
	source := &packetTimeoutReader{data: bytes.NewReader(packetReadWire(false, false, [][]byte{[]byte("partial")})), partial: true}
	s := &packetReadState{}
	_, _, first := s.read(source, false, batchTarget, nil, make([]byte, 10))
	if _, _, next := s.read(source, false, batchTarget, nil, make([]byte, 10)); next != first || !errors.Is(next, os.ErrDeadlineExceeded) {
		t.Fatal("partial header lost terminal error")
	}
}
