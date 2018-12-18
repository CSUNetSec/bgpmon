package util

import (
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"github.com/google/uuid"
	"net"
	"time"
)

func GetTimeColIP(pb *pb.WriteRequest) (net.IP, time.Time) {
	secs := time.Unix(int64(pb.GetBgpCapture().GetTimestamp()), 0)
	colip := net.IP(pb.GetBgpCapture().GetLocalIp().Ipv4)
	return colip, secs
}

func GetUpdateID() []byte {
	ret, _ := uuid.New().MarshalBinary()
	return ret
}

// What if it's not IPv4?
func GetPeerIP(wr *pb.WriteRequest) net.IP {
	cap := wr.GetBgpCapture()
	return net.IP(cap.GetPeerIp().Ipv4)
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
