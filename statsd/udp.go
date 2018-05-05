package statsd

import (
	"errors"
	"net"
	"time"
)

// udpWriter is an internal class wrapping around management of UDP connection
type udpWriter struct {
	*net.UDPConn
}

// New returns a pointer to a new udpWriter given an addr in the format "hostname:port".
func newUDPWriter(addr string) (udpWriter, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return udpWriter{}, err
	}
	conn, err := net.DialUDP("udp", nil, udpAddr)
	return udpWriter{UDPConn: conn}, err
}

// SetWriteTimeout is not needed for UDP, returns error
func (udpWriter) SetWriteTimeout(d time.Duration) error {
	return errors.New("SetWriteTimeout: not supported for UDP connections")
}
