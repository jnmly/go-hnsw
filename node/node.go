package node

import (
	//"fmt"
	"sync"
)

type Node struct {
	sync.RWMutex
	P            Point
	Level        uint64
	Friends      [][]NodeRef
	reverseLinks map[uint64]*link
	id           NodeRef
}

type link struct {
	nodes map[NodeRef]bool
}

type Point []float32
type NodeRef uint64

func (a Point) Size() int {
	return len(a) * 4
}

func NewNode(p Point, level uint64, friends [][]NodeRef, id NodeRef) *Node {
	n := &Node{}
	n.reverseLinks = make(map[uint64]*link)
	n.P = p
	n.id = id

	if friends != nil {
		n.Friends = friends
		n.Level = level
	} else {
		n.Level = 0
	}

	return n
}

func (n *Node) GetFriends(level uint64) []NodeRef {
	if uint64(len(n.Friends)) < level+1 {
		return make([]NodeRef, 0)
	}
	return n.Friends[level]
}

func (n *Node) FriendLevelCount() uint64 {
	return uint64(len(n.Friends))
}

func (n *Node) FriendCountAtLevel(level uint64) uint64 {
	return uint64(len(n.Friends[level]))
}

func (n *Node) AddReverseLink(other NodeRef, level uint64) {
	if n.reverseLinks[level] == nil {
		n.reverseLinks[level] = &link{
			nodes: make(map[NodeRef]bool),
		}
	}
	n.reverseLinks[level].nodes[other] = true
}

func (n *Node) UnlinkFromFriends(allnodes map[NodeRef]*Node) {
	for level, m := range n.reverseLinks {
		for node, _ := range m.nodes {
			xother := allnodes[node]
			if xother == nil {
				continue
			}
			nodes := xother.Friends[level]
			for j, x := range nodes {
				if x == n.GetId() {
					// exclude me from array
					xother.Friends[level] = append(xother.Friends[level][:j], xother.Friends[level][j+1:]...)
				}
			}
		}
	}
}

func (n *Node) GetId() NodeRef {
	return n.id
}
