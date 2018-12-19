package util

import (
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"github.com/google/uuid"
	"net"
	"time"
)

func GetTimeColIP(pb *pb.WriteRequest) (time.Time, net.IP) {
	var colip net.IP
	secs := time.Unix(int64(pb.GetBgpCapture().GetTimestamp()), 0)
	locip := pb.GetBgpCapture().GetLocalIp()
	if locip != nil && locip.Ipv4 != nil {
		colip = net.IP(locip.Ipv4)
	} else if locip != nil && locip.Ipv6 != nil {
		colip = net.IP(locip.Ipv6)
	}
	return secs, colip
}

func GetUpdateID() []byte {
	ret, _ := uuid.New().MarshalBinary()
	return ret
}

func GetPeerIP(wr *pb.WriteRequest) net.IP {
	var pip net.IP
	capt := wr.GetBgpCapture()
	pipwrap := capt.GetPeerIp()
	if pipwrap != nil && pipwrap.Ipv4 != nil {
		pip = net.IP(pipwrap.Ipv4)
	} else if pipwrap != nil && pipwrap.Ipv6 != nil {
		pip = net.IP(pipwrap.Ipv6)
	}
	return pip
}

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

func GetNextHop(wr *pb.WriteRequest) net.IP {
	nh := wr.GetBgpCapture().GetUpdate().GetAttrs().GetNextHop()
	return net.IP(nh.Ipv4)
}

func GetOriginAs(wr *pb.WriteRequest) int {
	as := wr.GetBgpCapture().GetUpdate().GetAttrs().GetOrigin()
	return int(as)
}
