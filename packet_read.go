package mux

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/metacubex/sing/common/buf"
	E "github.com/metacubex/sing/common/exceptions"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

// All read APIs share framing state and scratch storage. A short destination
// consumes the entire datagram; malformed or truncated wire data is terminal.
type packetReadState struct {
	sync.Mutex
	scratch      [256]byte
	err          error
	options      N.ReadWaitOptions
	headerReader packetHeaderReader
}

type packetHeaderReader struct {
	upstream io.Reader
	consumed int
}

func (r *packetHeaderReader) Read(p []byte) (int, error) {
	n, err := r.upstream.Read(p)
	r.consumed += n
	return n, err
}
func (s *packetReadState) headerError(err error) error {
	var timeout net.Error
	if s.headerReader.consumed == 0 && errors.As(err, &timeout) && timeout.Timeout() {
		return err
	}
	s.err = err
	return err
}

func (s *packetReadState) header(r io.Reader, withAddr bool, destination M.Socksaddr, responseRead *bool) (int, M.Socksaddr, error) {
	if s.err != nil {
		return 0, M.Socksaddr{}, s.err
	}
	s.headerReader = packetHeaderReader{upstream: r}
	r = &s.headerReader
	defer func() { s.headerReader.upstream = nil }()
	if responseRead != nil && !*responseRead {
		response, err := ReadStreamResponse(r)
		if err != nil {
			return 0, M.Socksaddr{}, s.headerError(err)
		}
		if response.Status != statusSuccess {
			s.err = E.New("remote error: ", response.Message)
			return 0, M.Socksaddr{}, s.err
		}
		*responseRead = true
		s.headerReader.consumed = 0
	}
	if withAddr {
		var err error
		destination, err = M.SocksaddrSerializer.ReadAddrPortWithScratch(r, s.scratch[:])
		if err != nil {
			return 0, M.Socksaddr{}, s.headerError(err)
		}
		if !destination.IsValid() {
			s.err = E.New("invalid packet destination")
			return 0, M.Socksaddr{}, s.err
		}
	}
	if _, err := io.ReadFull(r, s.scratch[:2]); err != nil {
		return 0, M.Socksaddr{}, s.headerError(err)
	}
	return int(binary.BigEndian.Uint16(s.scratch[:2])), destination, nil
}

func (s *packetReadState) discard(r io.Reader, size int) error {
	for size > 0 {
		n := size
		if n > len(s.scratch) {
			n = len(s.scratch)
		}
		if _, err := io.ReadFull(r, s.scratch[:n]); err != nil {
			s.err = err
			return err
		}
		size -= n
	}
	return io.ErrShortBuffer
}

func (s *packetReadState) read(r io.Reader, withAddr bool, destination M.Socksaddr, responseRead *bool, p []byte) (int, M.Socksaddr, error) {
	s.Lock()
	defer s.Unlock()
	size, destination, err := s.header(r, withAddr, destination, responseRead)
	if err != nil {
		return 0, M.Socksaddr{}, err
	}
	if len(p) < size {
		return 0, destination, s.discard(r, size)
	}
	n, err := io.ReadFull(r, p[:size])
	if err != nil {
		s.err = err
	}
	return n, destination, err
}

func (s *packetReadState) readBuffer(r io.Reader, withAddr bool, destination M.Socksaddr, responseRead *bool, buffer *buf.Buffer) (M.Socksaddr, error) {
	s.Lock()
	defer s.Unlock()
	size, destination, err := s.header(r, withAddr, destination, responseRead)
	if err != nil {
		return M.Socksaddr{}, err
	}
	if buffer.FreeLen() < size {
		return destination, s.discard(r, size)
	}
	_, err = buffer.ReadFullFrom(r, size)
	if err != nil {
		s.err = err
	}
	return destination, err
}

func (s *packetReadState) wait(r io.Reader, withAddr bool, destination M.Socksaddr, responseRead *bool) (*buf.Buffer, M.Socksaddr, error) {
	s.Lock()
	defer s.Unlock()
	size, destination, err := s.header(r, withAddr, destination, responseRead)
	if err != nil {
		return nil, M.Socksaddr{}, err
	}
	buffer := s.options.NewBufferSize(size)
	if _, err = buffer.ReadFullFrom(r, size); err != nil {
		s.err = err
		buffer.Release()
		return nil, M.Socksaddr{}, err
	}
	s.options.PostReturn(buffer)
	return buffer, destination, nil
}
