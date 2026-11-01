package mux

import (
	"fmt"
	"net"
	"sort"
	"testing"
	"time"

	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

func BenchmarkPacketFrames(b *testing.B) {
	for _, withAddr := range []bool{false, true} {
		for _, count := range []int{1, 8} {
			b.Run(fmt.Sprintf("address=%v/batch=%d", withAddr, count), func(b *testing.B) {
				raw := &partialBatchConn{limit: -1}
				extended := bufio.NewExtendedConn(raw)
				var conn N.NetPacketConn
				if withAddr {
					conn = &clientPacketAddrConn{AbstractConn: extended, conn: extended, destination: batchTarget, requestWritten: true}
				} else {
					conn = &clientPacketConn{AbstractConn: extended, conn: extended, destination: batchTarget, requestWritten: true}
				}
				benchmarkPacketWrites(b, conn, count, false)
			})
		}
	}
}

func benchmarkPacketWrites(b *testing.B, conn N.NetPacketConn, count int, readBack bool) {
	b.Helper()
	packets := make([]*buf.Buffer, count)
	destinations := make([]M.Socksaddr, count)
	for i := range destinations {
		destinations[i] = batchTarget
	}
	received := buf.NewSize(1200)
	defer received.Release()
	b.SetBytes(int64(1200 * count))
	b.ReportAllocs()
	var samples []int64
	if readBack {
		samples = make([]int64, b.N)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		var start time.Time
		if readBack {
			start = time.Now()
		}
		for i := range packets {
			packets[i] = buf.NewSize(1500)
			packets[i].Resize(300, 1200)
		}
		if count == 1 {
			if err := conn.WritePacket(packets[0], batchTarget); err != nil {
				b.Fatal(err)
			}
		} else if _, err := conn.(N.PacketBatchWriterWithCount).WritePacketBatchCount(packets, destinations); err != nil {
			b.Fatal(err)
		}
		if readBack {
			for i := 0; i < count; i++ {
				received.Reset()
				if _, err := conn.ReadPacket(received); err != nil || received.Len() != 1200 {
					b.Fatalf("reply=%d %v", received.Len(), err)
				}
			}
		}
		if readBack {
			samples[n] = time.Since(start).Nanoseconds()
		}
	}
	b.StopTimer()
	if readBack {
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		b.ReportMetric(float64(samples[(len(samples)-1)*95/100]), "p95-ns")
		b.ReportMetric(float64(samples[(len(samples)-1)*99/100]), "p99-ns")
	}
}

func BenchmarkPacketTCPRoundTrip(b *testing.B) {
	for _, protocol := range []byte{ProtocolH2Mux, ProtocolSmux, ProtocolYAMux} {
		for _, count := range []int{1, 8} {
			name := map[byte]string{ProtocolH2Mux: "h2mux", ProtocolSmux: "smux", ProtocolYAMux: "yamux"}[protocol]
			b.Run(fmt.Sprintf("%s/batch=%d", name, count), func(b *testing.B) {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					b.Fatal(err)
				}
				defer listener.Close()
				done := make(chan error, 1)
				go func() {
					raw, err := listener.Accept()
					if err != nil {
						done <- err
						return
					}
					defer raw.Close()
					raw.SetDeadline(time.Now().Add(time.Minute))
					session, err := newServerSession(raw, protocol)
					if err != nil {
						done <- err
						return
					}
					defer session.Close()
					stream, err := session.Accept()
					if err != nil {
						done <- err
						return
					}
					defer stream.Close()
					request, err := ReadStreamRequest(stream)
					if err != nil {
						done <- err
						return
					}
					server := &serverPacketAddrConn{ExtendedConn: bufio.NewExtendedConn(stream)}
					if !request.PacketAddr {
						done <- fmt.Errorf("wrong packet mode")
						return
					}
					for {
						packet := buf.NewSize(1500)
						packet.Resize(300, 0)
						destination, err := server.ReadPacket(packet)
						if err != nil {
							packet.Release()
							done <- nil
							return
						}
						if err = server.WritePacket(packet, destination); err != nil {
							done <- err
							return
						}
					}
				}()
				raw, err := net.Dial("tcp", listener.Addr().String())
				if err != nil {
					b.Fatal(err)
				}
				defer raw.Close()
				raw.SetDeadline(time.Now().Add(time.Minute))
				session, err := newClientSession(raw, protocol)
				if err != nil {
					b.Fatal(err)
				}
				defer session.Close()
				stream, err := session.Open(5 * time.Second)
				if err != nil {
					b.Fatal(err)
				}
				extended := bufio.NewExtendedConn(stream)
				client := &clientPacketAddrConn{AbstractConn: extended, conn: extended, destination: batchTarget}
				benchmarkPacketWrites(b, client, count, true)
				client.Close()
				if err := <-done; err != nil {
					b.Fatal(err)
				}
			})
		}
	}
}
