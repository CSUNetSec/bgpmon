package db

import (
	//	"fmt"
	"io"
	"testing"
	"time"

	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon/v2"
	"github.com/CSUNetSec/protoparse/fileutil"
)

func writeFileToStream(fName string, ws WriteStream) (int, error) {
	mf, err := fileutil.NewMrtFileReader(fName, nil)
	if err != nil {
		return 0, err
	}
	defer mf.Close()

	parsed := 0
	for mf.Scan() {
		cap, err := mf.GetCapture()
		if err != nil {
			// This is a parse error, and it doesn't matter
			continue
		}

		if cap != nil {
			parsed++
			writeRequest := new(pb.WriteRequest)
			writeRequest.Type = pb.WriteRequest_BGP_CAPTURE
			writeRequest.BgpCapture = cap

			err = ws.Write(writeRequest)
			if err != nil {
				return parsed, err
			}
		}
	}

	err = ws.Flush()
	if err != nil {
		return parsed, err
	}
	return parsed, nil
}

func TestSingleWriteStream(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	session, err := openTestSession(1)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	stream, err := session.OpenWriteStream(SessionWriteCapture)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	_, err = writeFileToStream("../docs/sample_mrt", stream)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSingleReadStream(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	session, err := openTestSession(1)
	if err != nil {
		t.Fatalf("Error opening test session: %s", err)
	}
	defer session.Close()

	rf := ReadFilter{
		collector: "routeviews2",
		start:     time.Date(2013, time.January, 1, 0, 0, 0, 0, time.UTC),
		end:       time.Date(2013, time.January, 2, 1, 0, 0, 0, time.UTC),
	}

	stream, err := session.OpenReadStream(SessionReadCapture, rf)
	if err != nil {
		t.Fatalf("Error opening read stream: %s", err)
	}
	defer stream.Close()

	msgCt := 0
	_, err = stream.Read()
	for err == nil {
		//fmt.Printf("[%d] %v\n", msgCt, cap)
		msgCt++
		_, err = stream.Read()
	}

	if err != io.EOF {
		t.Fatalf("Stream failed: %s", err)
	}

	t.Logf("Total messages read: %d", msgCt)
}
