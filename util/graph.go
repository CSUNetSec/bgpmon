//utility functions to generate as graphs
package util

import (
	"bytes"
	"fmt"
	pb "github.com/CSUNetSec/netsec-protobufs/bgpmon"
	"strings"
)

func DedupAspath(stats *pb.BGPStats) *pb.BGPStats {
	var (
		totaspnum map[string]int
	)
	totaspnum = make(map[string]int)
	for _, v := range stats.DestAsToASPCounts {
		for k1, _ := range v.Count {
			if num, ok := totaspnum[k1]; ok {
				totaspnum[k1] = num + 1
				delete(v.Count, k1)
			} else {
				totaspnum[k1] = 1
			}
		}
	}
	return stats
}

//creates a large string from the historical map
//using bytes.Buffer to be optimal in space/time.
func AsStr2dot(stats *pb.BGPStats) string {
	var (
		buf  bytes.Buffer
		seen map[string]bool
	)
	seen = make(map[string]bool)
	buf.WriteString("strict graph G {\n")
	buf.WriteString("overlap=none\n")
	for k, v := range stats.DestAsToASPCounts {
		buf.WriteString(fmt.Sprintf("%s ", k))
		seen[k] = true
		for k1, _ := range v.Count {
			parts := strings.Split(k1, ",")
			for i := range parts {
				tstr := strings.TrimSpace(parts[i])
				if _, ok := seen[tstr]; !ok {
					buf.WriteString(fmt.Sprintf(" -- %s", parts[i]))
					seen[tstr] = true
				}
			}
		}
		buf.WriteString(";\n")
		for k, _ := range seen {
			delete(seen, k)
		}
	}
	buf.WriteString("}\n")
	return buf.String()
}
