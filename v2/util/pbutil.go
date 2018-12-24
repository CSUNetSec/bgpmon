package util

import (
	"errors"
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	pbcomm "github.com/CSUNetSec/netsec-protobufs/common"
	"github.com/google/uuid"
	"net"
	"time"
)

var (
	errnoip = errors.New("could not decode IP from protobuf IP wrapper")
)

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

func GetTimeColIP(pb *pb.WriteRequest) (time.Time, net.IP, error) {
	secs := time.Unix(int64(pb.GetBgpCapture().GetTimestamp()), 0)
	locip := pb.GetBgpCapture().GetLocalIp()
	colip, err := GetIPWrapper(locip)
	return secs, colip, err
}

func GetUpdateID() []byte {
	ret, _ := uuid.New().MarshalBinary()
	return ret
}

func GetPeerIP(wr *pb.WriteRequest) (net.IP, error) {
	capt := wr.GetBgpCapture()
	pipwrap := capt.GetPeerIp()
	pip, err := GetIPWrapper(pipwrap)
	return pip, err
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

func GetNextHop(wr *pb.WriteRequest) (net.IP, error) {
	nh := wr.GetBgpCapture().GetUpdate().GetAttrs().GetNextHop()
	nhip, err := GetIPWrapper(nh)
	return nhip, err
}

func GetOriginAs(wr *pb.WriteRequest) int {
	as := wr.GetBgpCapture().GetUpdate().GetAttrs().GetOrigin()
	return int(as)
}
