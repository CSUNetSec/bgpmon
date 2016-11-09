package bgp

import (
	"errors"
	"net"
	"time"
)

const (
	timeBucketInterval = 86400
)

func getIPRange(ip net.IP, mask int) (net.IP, net.IP, error) {
	var maskBytes []byte
	if ip.To4() != nil {
		maskBytes = net.CIDRMask(mask, 32)
	} else {
		maskBytes = net.CIDRMask(mask, 128)
	}

	minIPAddress := ip.Mask(maskBytes)
	maxIPAddress := ip.Mask(maskBytes)
	for i := 0; i < len(maskBytes); i++ {
		maxIPAddress[i] = maxIPAddress[i] | ^maskBytes[i]
	}

	return minIPAddress, maxIPAddress, nil
}

func getTimeBuckets(startTime, endTime time.Time) ([]time.Time, error) {
	if endTime.Before(startTime) {
		return nil, errors.New("end time just be after start time")
	}

	startTimeBucket := time.Unix(startTime.Unix()-(startTime.Unix()%timeBucketInterval), 0)
	endTimeBucket := time.Unix(endTime.Unix()-(endTime.Unix()%timeBucketInterval), 0)

	timeBuckets := []time.Time{startTimeBucket}
	for !startTimeBucket.Equal(endTimeBucket) {
		timeBuckets = append(timeBuckets, startTimeBucket)
		startTimeBucket = startTimeBucket.Add(time.Duration(timeBucketInterval) * time.Second)
	}

	return timeBuckets, nil
}

func getTimeBucket(t time.Time) time.Time {
    return time.Unix(t.Unix() - (t.Unix() % timeBucketInterval), 0)
}
