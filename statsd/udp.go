package statsd

import (
	"errors"
	"net"
	"time"
)

// udpWriter is an internal class wrapping around management of UDP connection
type udpWriter struct {
	*net.UDPConn
	mtu int
}

// New returns a pointer to a new udpWriter given an addr in the format "hostname:port".
func newUDPWriter(addr string) (udpWriter, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return udpWriter{}, err
	}

	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return udpWriter{}, err
	}

	return udpWriter{
		UDPConn: conn,
		mtu:     mtuByLocalAddr(udpAddr.IP),
	}, nil
}

// SetWriteTimeout is not needed for UDP, returns error
func (udpWriter) SetWriteTimeout(d time.Duration) error {
	return errors.New("SetWriteTimeout: not supported for UDP connections")
}

func (w udpWriter) MTU() int {
	return w.mtu
}

func mtuByLocalAddr(local net.IP) int {
	ifaces, _ := net.Interfaces()

	for _, iface := range ifaces {
		addrs, _ := iface.Addrs()
		for _, addr := range addrs {
			addr, ok := addr.(*net.IPNet)
			if ok && addr.Contains(local) {
				return iface.MTU - 28
			}
		}
	}

	return OptimalPayloadSize
}
