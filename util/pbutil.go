package util

import (
	"errors"
	"net"
	"time"

	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	pbcomm "github.com/CSUNetSec/netsec-protobufs/common"
)

var (
	errnoip   = errors.New("could not decode IP from protobuf IP wrapper")
	errnilcap = errors.New("nil BGP capture provided")
)

// GetIPWrapper returns a net.IP from a pbcomm.IPAddressWrapper
func GetIPWrapper(pip *pbcomm.IPAddressWrapper) (net.IP, error) {
	if pip == nil || (pip.Ipv4 == nil && pip.Ipv6 == nil) {
		return nil, errnoip
	}

	var ret net.IP
	if pip.Ipv4 != nil {
		ret = net.IP(pip.Ipv4)
	} else if pip.Ipv6 != nil {
		ret = net.IP(pip.Ipv6)
	}

	return ret, nil
}

// GetTimeColIP returns the time and collector IP from a capture WriteRequest
func GetTimeColIP(cap *pb.BGPCapture) (time.Time, net.IP, error) {
	if cap == nil {
		return time.Now(), nil, errnilcap
	}
	secs := time.Unix(int64(cap.GetTimestamp()), 0)

	locip := cap.GetLocalIp()
	colip, err := GetIPWrapper(locip)
	return secs, colip, err
}

// GetPeerIP returns a PeerIP from a capture WriteRequest
func GetPeerIP(cap *pb.BGPCapture) (net.IP, error) {
	if cap == nil {
		return nil, errnilcap
	}

	pip, err := GetIPWrapper(cap.GetPeerIp())
	return pip, err
}

// GetAsPath returns an AS path from a capture WriteRequest
// XXX: consider returning an error
func GetAsPath(cap *pb.BGPCapture) []int {
	if cap == nil {
		return nil
	}

	segments := cap.GetUpdate().GetAttrs().GetAsPath()

	path := []int{}
	for _, s := range segments {
		if s.AsSet != nil {
			for _, as := range s.AsSet {
				path = append(path, int(as))
			}
		}
		if s.AsSeq != nil {
			for _, as := range s.AsSeq {
				path = append(path, int(as))
			}
		}
	}

	return path
}

// GetNextHop returns the IP of the next hop from a capture WriteRequest
func GetNextHop(cap *pb.BGPCapture) (net.IP, error) {
	if cap == nil {
		return nil, errnilcap
	}

	nh := cap.GetUpdate().GetAttrs().GetNextHop()
	nhip, err := GetIPWrapper(nh)
	return nhip, err
}

// GetOriginAs returns the AS at index 0 of the ASPath from a capture WriteRequest
func GetOriginAs(cap *pb.BGPCapture) int {
	path := GetAsPath(cap)
	if path == nil {
		return 0
	}
	return path[0]
}

// GetAdvertizedPrefixes returns the advertized routes from a capture WriteRequest
func GetAdvertizedPrefixes(cap *pb.BGPCapture) ([]*net.IPNet, error) {
	if cap == nil {
		return nil, errnilcap
	}

	routes := cap.GetUpdate().GetAdvertizedRoutes()
	if routes == nil {
		return nil, errors.New("No advertized prefixes")
	}

	return getPrefixListAsIPNet(routes.Prefixes)
}

// GetWithdrawnPrefixes returns the withdrawn routes from a captured WriteRequest
func GetWithdrawnPrefixes(cap *pb.BGPCapture) ([]*net.IPNet, error) {
	if cap == nil {
		return nil, errnilcap
	}

	routes := cap.GetUpdate().GetWithdrawnRoutes()
	if routes == nil {
		return nil, errors.New("No withdrawn prefixes")
	}

	return getPrefixListAsIPNet(routes.Prefixes)
}

func getPrefixListAsIPNet(prefs []*pbcomm.PrefixWrapper) ([]*net.IPNet, error) {
	if prefs == nil {
		return nil, errors.New("no prefixes provided")
	}
	var ret []*net.IPNet
	for _, pref := range prefs {
		net, err := getPrefixAsIPNet(pref)

		// Should this return an empty set or just ignore this entry?
		if err != nil {
			return nil, err
		}

		ret = append(ret, net)
	}

	return ret, nil
}

func getPrefixAsIPNet(pw *pbcomm.PrefixWrapper) (*net.IPNet, error) {
	ip, err := GetIPWrapper(pw.Prefix)
	if err != nil {
		return nil, err
	}
	var mask net.IPMask
	// This is true if it's an IPv4 or a special IPv6 address, might need to be fixed
	if ip.To4() != nil {
		mask = net.CIDRMask(int(pw.Mask), 32)
	} else {
		mask = net.CIDRMask(int(pw.Mask), 128)
	}

	return &net.IPNet{IP: ip, Mask: mask}, nil
}

//GetProtoMsg returns a byte array representing the capture from a WritRequest
func GetProtoMsg(cap *pb.BGPCapture) []byte {
	if cap == nil {
		return nil
	}
	return []byte(cap.String())
}
