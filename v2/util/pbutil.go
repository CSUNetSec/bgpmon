package util

import (
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"net"
	"time"
)

func GetTimeColIP(pb *pb.WriteRequest) (net.IP, time.Time) {
	secs := time.Unix(int64(pb.GetBgpCapture().GetTimestamp()), 0)
	colip := net.IP(pb.GetBgpCapture().GetLocalIp().Ipv4)
	return colip, secs
}
