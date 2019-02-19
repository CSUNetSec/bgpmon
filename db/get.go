package db

/*
import (
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"time"
)

func mkPbReplyErr(errstr string) pb.GetReply {
	pb := pb.GetReply{
		Error: errstr,
	}
	return pb
}

//GetCaptures services a GetCapture request by returning a channel of
//capturedMessages. These are fetched from the db and are not parsed.
//They are passed as protobuf streams. It invokes a new goroutine to
//be able to not block on the channel send.
func GetCaptures(sess *Session, req *pb.GetRequest) chan pb.GetReply {
	ret := make(chan pb.GetReply)
	go func() {
		defer close(ret)
		ex := newDbSessionExecutor(sess.db, sess.dbo) //XXX isn't this clutter? we need to refactor
		tstamp := req.GetStartTimestamp()
		estamp := req.GetEndTimestamp()
		colname := req.GetCollectorName()
		d1 := time.Unix(int64(tstamp), 0) // errors here should have been checked by the cli
		d2 := time.Unix(int64(estamp), 0)

		msg := getCapMessage{capTableMessage{CommonMessage: newMessage(), tableCol: colname, sDate: d1, eDate: d2}}
		capc := getCaptures(ex, msg)
		for capt := range capc {
			//XXX chunk
			dblogger.Infof("i received a capture:%v . making it protobuf", capt)
			ret <- pb.GetReply{}
		}
	}()
	return ret
}
*/
