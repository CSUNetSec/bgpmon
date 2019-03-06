package util

import (
	"errors"
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	pbcomm "github.com/CSUNetSec/netsec-protobufs/common"
	"net"
	"time"
)

var (
	errnoip = errors.New("could not decode IP from protobuf IP wrapper")
)

// GetIPWrapper returns a net.IP from a pbcomm.IPAddressWrapper
func GetIPWrapper(pip *pbcomm.IPAddressWrapper) (ret net.IP, err error) {
	if pip != nil && pip.Ipv4 != nil {
		ret = net.IP(pip.Ipv4)
	} else if pip != nil && pip.Ipv6 != nil {
		ret = net.IP(pip.Ipv6)
	} else {
		err = errnoip
	}
	return
}

// GetTimeColIP returns the time and collector IP from a capture WriteRequest
func GetTimeColIP(pb *pb.WriteRequest) (time.Time, net.IP, error) {
	secs := time.Unix(int64(pb.GetBgpCapture().GetTimestamp()), 0)
	locip := pb.GetBgpCapture().GetLocalIp()
	colip, err := GetIPWrapper(locip)
	return secs, colip, err
}

// GetPeerIP returns a PeerIP from a capture WriteRequest
func GetPeerIP(wr *pb.WriteRequest) (net.IP, error) {
	capt := wr.GetBgpCapture()
	pipwrap := capt.GetPeerIp()
	pip, err := GetIPWrapper(pipwrap)
	return pip, err
}

// GetAsPath returns an AS path from a capture WriteRequest
func GetAsPath(wr *pb.WriteRequest) []int {
	segments := wr.GetBgpCapture().GetUpdate().GetAttrs().GetAsPath()

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
func GetNextHop(wr *pb.WriteRequest) (net.IP, error) {
	nh := wr.GetBgpCapture().GetUpdate().GetAttrs().GetNextHop()
	nhip, err := GetIPWrapper(nh)
	return nhip, err
}

// GetOriginAs returns the AS at index 0 of the ASPath from a capture WriteRequest
func GetOriginAs(wr *pb.WriteRequest) int {
	as := wr.GetBgpCapture().GetUpdate().GetAttrs().GetOrigin()
	return int(as)
}

// GetAdvertizedPrefixes returns the advertized routes from a capture WriteRequest
func GetAdvertizedPrefixes(wr *pb.WriteRequest) ([]*net.IPNet, error) {
	routes := wr.GetBgpCapture().GetUpdate().GetAdvertizedRoutes()
	if routes == nil {
		return nil, errors.New("No advertized prefixes")
	}

	return getPrefixListAsIPNet(routes.Prefixes)
}

// GetWithdrawnPrefixes returns the withdrawn routes from a captured WriteRequest
func GetWithdrawnPrefixes(wr *pb.WriteRequest) ([]*net.IPNet, error) {
	routes := wr.GetBgpCapture().GetUpdate().GetWithdrawnRoutes()
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
func GetProtoMsg(wr *pb.WriteRequest) []byte {
	return []byte(wr.GetBgpCapture().String())
}
