package bgp

import (
	"bytes"
	"errors"
	"fmt"
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
	return time.Unix(t.Unix()-(t.Unix()%timeBucketInterval), 0)
}

/*
 * PrefixCache
 */
type PrefixCache struct {
	roots       []*PrefixNode
	prefixNodes []*PrefixNode
}

func NewPrefixCache() *PrefixCache {
	return &PrefixCache{
		roots:       []*PrefixNode{},
		prefixNodes: []*PrefixNode{},
	}
}

func (p *PrefixCache) AddPrefix(ipAddress net.IP, mask uint32, asNumber uint32) error {
	//create PrefxNode
	prefixNode := NewPrefixNode(ipAddress, mask, asNumber)

	//check if node already exists on prefix nodes slice
	found := false
	for _, node := range p.prefixNodes {
		if prefixNode.Equals(node) {
			found = true
			break
		}
	}

	if !found {
		p.prefixNodes = append(p.prefixNodes, prefixNode)
	}

	//check if prefixNode is subprefix/superprefix of a root
	removeIndex := -1
	for i, node := range p.roots {
		if prefixNode.Equals(node) {
			found := false
			for _, asNum := range node.asNumbers {
				if asNum == asNumber {
					found = true
				}
			}

			if !found {
				node.asNumbers = append(node.asNumbers, asNumber)
			}
			return nil
		} else if prefixNode.SubPrefix(node) {
			//find correct node to insert on
			insertNode := node
			found := true
			for found {
				found = false
				for _, child := range insertNode.children {
					if prefixNode.SubPrefix(child) {
						insertNode = child
						found = true
					} else if prefixNode.Equals(child) {
						//equals a child - add as number
						found := false
						for _, asNum := range child.asNumbers {
							if asNum == asNumber {
								found = true
							}
						}

						if !found {
							child.asNumbers = append(child.asNumbers, asNumber)
						}
						return nil
					}
				}
			}

			//check if it's a superprefix to any children
			superPrefixIndex := -1
			for i, child := range insertNode.children {
				if prefixNode.SuperPrefix(child) {
					superPrefixIndex = i
					break
				}
			}

			if superPrefixIndex != -1 {
				prefixNode.parent = insertNode
				insertNode.children[superPrefixIndex].parent = prefixNode

				prefixNode.children = append(prefixNode.children, insertNode.children[superPrefixIndex])
				insertNode.children = append(insertNode.children[:superPrefixIndex], insertNode.children[superPrefixIndex+1:]...)
			} else {
				prefixNode.parent = insertNode
				insertNode.children = append(insertNode.children, prefixNode)
			}

			return nil
		} else if prefixNode.SuperPrefix(node) {
			//add prefixNode as superprefix to node
			node.parent = prefixNode
			prefixNode.children = append(prefixNode.children, node)

			removeIndex = i
			break
		}
	}

	if removeIndex != -1 {
		//remove value
		p.roots = append(p.roots[:removeIndex], p.roots[removeIndex+1:]...)
	}

	p.roots = append(p.roots, prefixNode)
	return nil
}

func (p *PrefixCache) Print() {
	for _, root := range p.roots {
		root.Print(0)
	}
}

type PrefixNode struct {
	ipAddress              net.IP
	mask                   uint32
	asNumbers              []uint32
	minAddress, maxAddress []byte
	parent                 *PrefixNode
	children               []*PrefixNode
}

func NewPrefixNode(ipAddress net.IP, mask uint32, asNumber uint32) *PrefixNode {
	minAddress, maxAddress, _ := getIPRange(ipAddress, int(mask))

	return &PrefixNode{
		ipAddress:  ipAddress,
		mask:       mask,
		asNumbers:  []uint32{asNumber},
		minAddress: minAddress,
		maxAddress: maxAddress,
		parent:     nil,
		children:   []*PrefixNode{},
	}
}

func (p *PrefixNode) Equals(prefixNode *PrefixNode) bool {
	if p.ipAddress.Equal(prefixNode.ipAddress) && p.mask == prefixNode.mask {
		return true
	}

	return false
}

func (p *PrefixNode) SubPrefix(prefixNode *PrefixNode) bool {
	//check if p.mask is shorter
	if p.mask <= prefixNode.mask {
		return false
	}

	//check if p.minAddress < prefixNode.minAddress or p.maxAddress > prefixNode.maxAddress
	if bytes.Compare(p.minAddress, prefixNode.minAddress) < 0 || bytes.Compare(p.maxAddress, prefixNode.maxAddress) > 0 {
		return false
	}

	return true
}

func (p *PrefixNode) SuperPrefix(prefixNode *PrefixNode) bool {
	//check if p.mask is longer
	if p.mask >= prefixNode.mask {
		return false
	}

	//check if p.minAddress > prefixNode.minAddress or p.maxAddress < prefixNode.maxAddress
	if bytes.Compare(p.minAddress, prefixNode.minAddress) > 0 || bytes.Compare(p.maxAddress, prefixNode.maxAddress) < 0 {
		return false
	}

	return true
}

func (p *PrefixNode) ValidAsNumber(asNumber uint32) bool {
	for _, asNum := range p.asNumbers {
		if asNum == asNumber {
			return true
		}
	}

	if p.parent != nil {
		return p.parent.ValidAsNumber(asNumber)
	} else {
		return false
	}
}

func (p *PrefixNode) Print(indent int) {
	for i := 0; i < indent; i++ {
		fmt.Printf("\t")
	}

	fmt.Printf("%s/%d : %v\n", p.ipAddress, p.mask, p.asNumbers)

	for _, child := range p.children {
		child.Print(indent + 1)
	}
}
