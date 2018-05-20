package dogstatsd

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
	overhead := udpOverhead
	if local.To4() == nil {
		overhead += ipv6Overhead
	} else {
		overhead += +ipv4Overhead
	}

	ifaces, _ := net.Interfaces()

	for _, iface := range ifaces {
		addrs, _ := iface.Addrs()
		for _, addr := range addrs {
			addr, ok := addr.(*net.IPNet)
			if ok && addr.Contains(local) {
				if iface.MTU == -1 {
					// Windows loopback MTU may be reported as -1
					return maxMTU - overhead
				}
				return iface.MTU - overhead
			}
		}
	}

	return typicalMTU - overhead
}

const (
	typicalMTU   = 1500
	maxMTU       = 65535 // technically IPv6 can be larger
	ipv4Overhead = 20
	ipv6Overhead = 40
	udpOverhead  = 8
)
