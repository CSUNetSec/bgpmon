package db

import (
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/CSUNetSec/protoparse/fileutil"
)

func RunAndLog(s func() error) {
	if err := s(); err != nil {
		fmt.Printf("Session failed to close: %s\n", err)
	}
}

const (
	maxWriteCt = 1000
)

var (
	testEntities = []*Entity{
		{
			Name:         "test1",
			Email:        "test1@test.com",
			OwnedOrigins: []int{1, 2, 3},
		},
		{
			Name:         "test2",
			Email:        "test2@test.com",
			OwnedOrigins: []int{4, 5, 6},
			OwnedPrefixes: []*net.IPNet{
				{
					IP:   net.IPv4(1, 2, 3, 0),
					Mask: net.CIDRMask(24, 32),
				},
			},
		},
	}
)

func writeFileToStream(fName string, ws WriteStream) (int, error) {
	mf, err := fileutil.NewMrtFileReader(fName, nil)
	if err != nil {
		return 0, err
	}
	defer mf.Close()

	parsed := 0
	for mf.Scan() && parsed < maxWriteCt {
		pbCap, err := mf.GetCapture()
		if err != nil {
			// This is a parse error, and it doesn't matter
			continue
		}

		if pbCap != nil {
			parsed++

			cap, err := NewCaptureFromPB(pbCap)
			if err != nil {
				continue
			}

			err = ws.Write(cap)
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
	defer RunAndLog(session.Close)

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

func TestEntityWriteStream(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	session, err := openTestSession(1)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	stream, err := session.OpenWriteStream(SessionWriteEntity)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	for _, e := range testEntities {
		err = stream.Write(e)
		if err != nil {
			t.Fatal(err)
		}
	}

	if err = stream.Flush(); err != nil {
		t.Fatal(err)
	}

}

func TestEntityReadStream(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	session, err := openTestSession(1)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	stream, err := session.OpenReadStream(SessionReadEntity, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	msgCt := 0
	for stream.Read() {
		msgCt++
		t.Logf("Read entity: %+v", stream.Data())
	}

	if stream.Err() != nil {
		t.Fatal(stream.Err())
	}

	t.Logf("Total entities read: %d", msgCt)
}

func TestEntityNameFilters(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	session, err := openTestSession(1)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	for _, v := range testEntities {
		entOpts := NewEntityFilterOptions(v.Name)

		stream, err := session.OpenReadStream(SessionReadEntity, entOpts)
		if err != nil {
			t.Fatal(err)
		}

		hasData := stream.Read()
		if !hasData {
			t.Fatalf("Expected data from stream, found none. Error: %s", stream.Err())
			stream.Close()
		}
		readEnt := stream.Data().(*Entity)

		if readEnt.Name != v.Name {
			t.Fatalf("Expected name: %s, Got: %s", v.Name, readEnt.Name)
		}

		hasData = stream.Read()
		if hasData {
			t.Fatalf("Expected no data, found some. Data: %+v", stream.Data())
		}
		stream.Close()
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
	defer RunAndLog(session.Close)

	collector := "routeviews2"
	// These dates correspond to the data in the sample MRT file above.
	start := time.Date(2013, time.January, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2013, time.January, 2, 1, 0, 0, 0, time.UTC)

	cfo := NewCaptureFilterOptions(collector, start, end)

	stream, err := session.OpenReadStream(SessionReadCapture, cfo)
	if err != nil {
		t.Fatalf("Error opening read stream: %s", err)
	}
	defer stream.Close()

	msgCt := 0
	for stream.Read() {
		msgCt++
	}

	if err := stream.Err(); err != nil && err != io.EOF {
		t.Fatalf("Stream failed: %s", err)
	}

	t.Logf("Total messages read: %d", msgCt)
}

func TestCaptureOriginFilter(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	session, err := openTestSession(1)
	if err != nil {
		t.Fatalf("Error opening test session: %s", err)
	}
	defer RunAndLog(session.Close)

	collector := "routeviews2"
	// These dates correspond to the data in the sample MRT file above.
	start := time.Date(2013, time.January, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2013, time.January, 2, 1, 0, 0, 0, time.UTC)

	cfo := NewCaptureFilterOptions(collector, start, end)
	// This origin was manually observed in the data
	filterOrigin := 29838
	cfo.SetOrigin(filterOrigin)

	stream, err := session.OpenReadStream(SessionReadCapture, cfo)
	if err != nil {
		t.Fatalf("Error opening read stream: %s", err)
	}
	defer stream.Close()

	msgCt := 0
	for stream.Read() {
		cap := stream.Data().(*Capture)
		if cap.Origin != filterOrigin {
			t.Fatalf("[%d] Expected origin: %d, Got: %d", msgCt, filterOrigin, cap.Origin)
		}
		msgCt++
	}

	if err := stream.Err(); err != nil {
		t.Fatalf("Stream failed: %s", err)
	}

	t.Logf("Total messages read: %d", msgCt)
}

func TestCapturePrefixFilter(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	session, err := openTestSession(1)
	if err != nil {
		t.Fatalf("Error opening test session: %s", err)
	}
	defer RunAndLog(session.Close)

	collector := "routeviews2"
	// These dates correspond to the data in the sample MRT file above.
	start := time.Date(2013, time.January, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2013, time.January, 2, 1, 0, 0, 0, time.UTC)

	cfo := NewCaptureFilterOptions(collector, start, end)
	// This prefix was manually observed in the data
	filterPrefs := []string{"49.248.72.0/21", "84.205.76.0/24"}
	nets := make([]*net.IPNet, len(filterPrefs))
	for i, v := range filterPrefs {
		_, net, err := net.ParseCIDR(v)
		if err != nil {
			t.Fatal(err)
		}
		nets[i] = net
	}

	cfo.AllowAdvPrefixes(nets...)

	stream, err := session.OpenReadStream(SessionReadCapture, cfo)
	if err != nil {
		t.Fatalf("Error opening read stream: %s", err)
	}
	defer stream.Close()

	msgCt := 0
	for stream.Read() {
		cap := stream.Data().(*Capture)

		found := false
		for _, v := range cap.Advertised {
			prefStr := v.String()

			for _, v := range filterPrefs {
				if prefStr == v {
					found = true
					break
				}
			}

			if found {
				break
			}
		}

		if !found {
			t.Fatalf("Didn't find any of the filtered prefixes.")
		}

		msgCt++
	}

	if err := stream.Err(); err != nil {
		t.Fatalf("Stream failed: %s", err)
	}

	t.Logf("Total messages read: %d", msgCt)
}
