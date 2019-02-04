package db

import (
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
)

//GetCaptures services a GetCapture request by returning a channel of
//capturedMessages. These are fetched from the db and are not parsed.
//They are passed are protobuf streams.
func GetCaptures(sess *Session, req *pb.GetRequest) chan SerializableReply {
	ret := make(chan SerializableReply)
	defer close(ret)
	for i := 0; i < 10; i++ {
		ret <- newCaptureReply(nil, nil)
	}
	return ret
}
