package mux

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

var batchTarget = M.ParseSocksaddr("127.0.0.1:5353")

func batchInputs(payloads ...string) ([]*buf.Buffer, []M.Socksaddr) {
	buffers := make([]*buf.Buffer, len(payloads))
	targets := make([]M.Socksaddr, len(payloads))
	for i, p := range payloads {
		buffers[i] = buf.NewSize(len(p) + 1)
		buffers[i].WriteString(p)
		targets[i] = batchTarget
	}
	return buffers, targets
}
func checkReleased(t *testing.T, buffers []*buf.Buffer) {
	t.Helper()
	for _, b := range buffers {
		if b.RawCap() != 0 {
			t.Fatal("owned input retained")
		}
	}
}
func TestPacketBatchBothDirections(t *testing.T) {
	for _, withAddr := range []bool{false, true} {
		left, right := net.Pipe()
		left.SetDeadline(time.Now().Add(5 * time.Second))
		right.SetDeadline(time.Now().Add(5 * time.Second))
		var client N.NetPacketConn
		extended := bufio.NewExtendedConn(left)
		if withAddr {
			client = &clientPacketAddrConn{AbstractConn: extended, conn: extended, destination: batchTarget}
		} else {
			client = &clientPacketConn{AbstractConn: extended, conn: extended, destination: batchTarget}
		}
		payloads := []string{"", "one", string(bytes.Repeat([]byte{0x66}, 60000)), "tail"}
		bs, ds := batchInputs(payloads...)
		if withAddr {
			ds[1] = M.ParseSocksaddr("[::1]:5354")
		}
		done := make(chan error, 1)
		go func() {
			defer right.Close()
			request, err := ReadStreamRequest(right)
			if err != nil {
				done <- err
				return
			}
			if request.PacketAddr != withAddr {
				done <- errors.New("wrong packet mode")
				return
			}
			var server N.NetPacketConn
			if withAddr {
				server = &serverPacketAddrConn{ExtendedConn: bufio.NewExtendedConn(right)}
			} else {
				server = &serverPacketConn{ExtendedConn: bufio.NewExtendedConn(right), destination: request.Destination}
			}
			replies := make([]*buf.Buffer, len(payloads))
			targets := make([]M.Socksaddr, len(payloads))
			for i, want := range payloads {
				b := buf.NewSize(65535)
				addr, err := server.ReadPacket(b)
				if err != nil || string(b.Bytes()) != want || addr != ds[i] {
					b.Release()
					buf.ReleaseMulti(replies)
					done <- errors.New("packet boundary or address lost")
					return
				}
				replies[i] = b
				targets[i] = addr
			}
			n, err := server.(N.PacketBatchWriterWithCount).WritePacketBatchCount(replies, targets)
			if err == nil && n != len(payloads) {
				err = io.ErrShortWrite
			}
			done <- err
		}()
		n, err := client.(N.PacketBatchWriterWithCount).WritePacketBatchCount(bs, ds)
		if err != nil || n != len(payloads) {
			t.Fatalf("prefix=%d err=%v", n, err)
		}
		checkReleased(t, bs)
		for i, want := range payloads {
			b := buf.NewSize(65535)
			addr, err := client.ReadPacket(b)
			if err != nil || string(b.Bytes()) != want || addr != ds[i] {
				t.Fatalf("reply size=%d want=%d addr=%v err=%v", b.Len(), len(want), addr, err)
			}
			b.Release()
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		client.Close()
	}
}

type partialBatchConn struct {
	net.Conn
	limit   int
	failure error
	calls   int
	closed  bool
}

func (c *partialBatchConn) Write(p []byte) (int, error) {
	c.calls++
	n := len(p)
	if c.limit >= 0 && n > c.limit {
		n = c.limit
	}
	return n, c.failure
}
func (c *partialBatchConn) Close() error { c.closed = true; return nil }
func TestPacketBatchPartialLazyHeaderAndRetry(t *testing.T) {
	request := StreamRequest{Network: N.NetworkUDP, Destination: batchTarget, PacketAddr: true}
	first := streamRequestLen(request) + M.SocksaddrSerializer.AddrPortLen(batchTarget) + 2
	for _, tc := range []struct {
		limit, want int
		failure     error
	}{{first - 1, 0, io.ErrClosedPipe}, {first, 1, io.ErrClosedPipe}, {first + 1, 1, nil}, {-1, 3, io.ErrClosedPipe}} {
		raw := &partialBatchConn{limit: tc.limit, failure: tc.failure}
		extended := bufio.NewExtendedConn(raw)
		client := &clientPacketAddrConn{AbstractConn: extended, conn: extended, destination: batchTarget}
		buffers, targets := batchInputs("", "one", "tail")
		n, err := client.WritePacketBatchCount(buffers, targets)
		wantErr := tc.failure
		if wantErr == nil {
			wantErr = io.ErrShortWrite
		}
		if n != tc.want || !errors.Is(err, wantErr) || !raw.closed {
			t.Fatalf("n=%d err=%v", n, err)
		}
		checkReleased(t, buffers)
		if _, err = client.WriteTo([]byte("retry"), batchTarget); err == nil || raw.calls != 1 {
			t.Fatal("scalar retried failed packet stream")
		}
	}
}
func TestPacketBatchValidationAndChunkBound(t *testing.T) {
	raw := &partialBatchConn{limit: -1}
	extended := bufio.NewExtendedConn(raw)
	client := &clientPacketAddrConn{AbstractConn: extended, conn: extended, destination: batchTarget}
	buffers, targets := batchInputs("valid", "invalid")
	targets[1] = M.Socksaddr{}
	n, err := client.WritePacketBatchCount(buffers, targets)
	if n != 0 || err == nil || raw.calls != 0 || !client.NeedHandshake() {
		t.Fatal("validation advanced request state")
	}
	checkReleased(t, buffers)
	payloads := make([]string, 70)
	buffers, targets = batchInputs(payloads...)
	n, err = client.WritePacketBatchCount(buffers, targets)
	if n != 70 || err != nil || raw.calls != 2 {
		t.Fatalf("n=%d calls=%d err=%v", n, raw.calls, err)
	}
	checkReleased(t, buffers)
}

type blockedBatchConn struct {
	net.Conn
	started, closed chan struct{}
	once            sync.Once
}

func (c *blockedBatchConn) Write([]byte) (int, error) {
	close(c.started)
	<-c.closed
	return 0, net.ErrClosed
}
func (c *blockedBatchConn) Close() error { c.once.Do(func() { close(c.closed) }); return nil }
func TestPacketBatchCloseUnblocks(t *testing.T) {
	raw := &blockedBatchConn{started: make(chan struct{}), closed: make(chan struct{})}
	extended := bufio.NewExtendedConn(raw)
	client := &clientPacketAddrConn{AbstractConn: extended, conn: extended, destination: batchTarget}
	defer client.Close()
	buffers, targets := batchInputs("blocked")
	done := make(chan error, 1)
	go func() { _, err := client.WritePacketBatchCount(buffers, targets); done <- err }()
	select {
	case <-raw.started:
	case <-time.After(time.Second):
		t.Fatal("write not started")
	}
	client.Close()
	select {
	case err := <-done:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("close waited for write lock")
	}
	checkReleased(t, buffers)
}

func TestPacketBatchConcurrentScalarAndBatch(t *testing.T) {
	raw := &partialBatchConn{limit: -1}
	extended := bufio.NewExtendedConn(raw)
	client := &clientPacketAddrConn{AbstractConn: extended, conn: extended, destination: batchTarget}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			if index%2 == 0 {
				_, err := client.WriteTo([]byte("scalar"), batchTarget)
				if err != nil {
					t.Error(err)
				}
			} else {
				bs, ds := batchInputs("one", "two")
				n, err := client.WritePacketBatchCount(bs, ds)
				if err != nil || n != 2 {
					t.Errorf("n=%d err=%v", n, err)
				}
			}
		}(i)
	}
	wg.Wait()
	if raw.calls != 8 {
		t.Fatalf("wire calls=%d", raw.calls)
	}
}
