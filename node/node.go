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
	reverseLinks []*link
	id           NodeRef
}

type link struct {
	othernode  NodeRef
	otherlevel uint64
}

type Point []float32
type NodeRef uint64

func (a Point) Size() int {
	return len(a) * 4
}

func NewNode(p Point, level uint64, friends [][]NodeRef, id NodeRef) *Node {
	if friends != nil {
		return &Node{P: p, Level: level, Friends: friends, id: id}
	} else {
		return &Node{Level: 0, P: p, id: id}
	}
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
	if n.reverseLinks == nil {
		n.reverseLinks = make([]*link, 0)
	}

	n.reverseLinks = append(n.reverseLinks,
		&link{
			othernode:  other,
			otherlevel: level,
		},
	)
}

func (n *Node) UnlinkFromFriends(allnodes map[NodeRef]*Node) {
	for _, other := range n.reverseLinks {
		xother := allnodes[other.othernode]
		nodes := xother.Friends[other.otherlevel]
		for j, x := range nodes {
			if x == n.GetId() {
				// exclude me from array
				xother.Friends[other.otherlevel] = append(xother.Friends[other.otherlevel][:j], xother.Friends[other.otherlevel][j+1:]...)
			}
		}
	}
}

func (n *Node) GetId() NodeRef {
	return n.id
}
