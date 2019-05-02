package util

// pbutil.go contains utility functions to extract fields of interest from the
// protocol buffer representations of data to native golang types that can then
// be used by bgpmon.

import (
	"errors"
	"net"
	"time"

	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	pbcomm "github.com/CSUNetSec/netsec-protobufs/common"
)

var (
	ErrNoIP                 = errors.New("could not decode IP from protobuf IP wrapper")
	ErrNilCap               = errors.New("nil BGP capture provided")
	ErrNilPrefWrap          = errors.New("nil Prefix wrapper provided")
	ErrNoASPath             = errors.New("could not extract an Autonomous System Path")
	ErrNoAdvertisedPrefixes = errors.New("could not extract any Advertised Prefixes")
	ErrNoWithdrawnPrefixes  = errors.New("could not extract any Withdrawn Prefixes")
)

// GetIPWrapper returns a net.IP and possibly an error  from the protobuf IP address wrapper.
func GetIPWrapper(pIP *pbcomm.IPAddressWrapper) (net.IP, error) {
	if pIP == nil || (pIP.IPv4 == nil && pIP.IPv6 == nil) {
		return nil, ErrNoIP
	}

	var ret net.IP
	if pIP.IPv4 != nil {
		ret = net.IP(pIP.IPv4)
	} else if pIP.IPv6 != nil {
		ret = net.IP(pIP.IPv6)
	}

	return ret, nil
}

// GetTimeColIP returns the time of the capture, the collector IP and possibly an error
// from a protobuf BGP capture.
func GetTimeColIP(cap *pb.BGPCapture) (time.Time, net.IP, error) {
	if cap == nil {
		return time.Now(), nil, ErrNilCap
	}
	secs := time.Unix(int64(cap.GetTimestamp()), 0)

	locIP := cap.GetLocal_IP()
	colIP, err := GetIPWrapper(locIP)
	return secs, colIP, err
}

// GetPeerIP returns a PeerIP and possibly and error  from a protobuf
// capture WriteRequest.
func GetPeerIP(cap *pb.BGPCapture) (net.IP, error) {
	if cap == nil {
		return nil, ErrNilCap
	}

	pIP, err := GetIPWrapper(cap.GetPeer_IP())
	return pIP, err
}

// GetASPath returns an Autonomous System path as an array of integers
// from a protobuf capture WriteRequest.
func GetASPath(cap *pb.BGPCapture) ([]int, error) {
	if cap == nil {
		return nil, ErrNilCap
	}

	segments := cap.GetUpdate().GetAttrs().GetASPath()

	if segments == nil {
		return nil, ErrNoASPath
	}

	var path []int
	for _, s := range segments {
		if s.ASSet != nil {
			for _, AS := range s.ASSet {
				path = append(path, int(AS))
			}
		}
		if s.ASSeq != nil {
			for _, AS := range s.ASSeq {
				path = append(path, int(AS))
			}
		}
	}

	return path, nil
}

// GetNextHop returns the IP and possibly an error  of the next hop
// from a protobuf capture WriteRequest.
func GetNextHop(cap *pb.BGPCapture) (net.IP, error) {
	if cap == nil {
		return nil, ErrNilCap
	}

	nh := cap.GetUpdate().GetAttrs().GetNextHop()
	nhIP, err := GetIPWrapper(nh)
	return nhIP, err
}

// GetOriginAS returns the origin AS as an integer (the AS at index 0 of the AS-Path)
// of the ASPath from a capture WriteRequest or possibly an error.
func GetOriginAS(cap *pb.BGPCapture) (int, error) {
	path, err := GetASPath(cap)
	if err != nil {
		return 0, err
	}
	return path[0], nil
}

// GetAdvertisedPrefixes returns the advertised routes as a slice of IPNet and possibly an error
// from a protobuf capture WriteRequest.
func GetAdvertisedPrefixes(cap *pb.BGPCapture) ([]*net.IPNet, error) {
	if cap == nil {
		return nil, ErrNilCap
	}

	routes := cap.GetUpdate().GetAdvertisedRoutes()
	if routes == nil {
		return nil, ErrNoAdvertisedPrefixes
	}

	return getPrefixListAsIPNet(routes.Prefixes)
}

// GetWithdrawnPrefixes returns the withdrawn routes as a slice of IPNet and possibly an error
// from a protobuf capture WriteRequest
func GetWithdrawnPrefixes(cap *pb.BGPCapture) ([]*net.IPNet, error) {
	if cap == nil {
		return nil, ErrNilCap
	}

	routes := cap.GetUpdate().GetWithdrawnRoutes()
	if routes == nil {
		return nil, ErrNoWithdrawnPrefixes
	}

	return getPrefixListAsIPNet(routes.Prefixes)
}

// getPrefixListAsIPNet returns the slice of IPNet and possibly an error from a
// slice of protobuf PrefixWrapper. In case an error is found during the decoding of any
// part of the prefix list, this function will return that error and an empty slice.
func getPrefixListAsIPNet(prefs []*pbcomm.PrefixWrapper) ([]*net.IPNet, error) {
	if prefs == nil {
		return nil, ErrNilPrefWrap
	}
	var ret []*net.IPNet
	for _, pref := range prefs {
		net, err := getPrefixAsIPNet(pref)

		// If there is error return an empty array and the underlying decoding error.
		if err != nil {
			return nil, err
		}

		ret = append(ret, net)
	}

	return ret, nil
}

// getPrefixAsIPNet returns the protobuf prefixwrapper to a native net.IPNet
// type and possibly returns an error.
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

// GetProtoMsg returns a byte array representing the capture from a protobuf capture WriteRequest.
func GetProtoMsg(cap *pb.BGPCapture) []byte {
	if cap == nil {
		return nil
	}
	return []byte(cap.String())
}
