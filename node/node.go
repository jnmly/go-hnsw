package node

import (
	//"fmt"
	"sync"
)

type Node struct {
	sync.RWMutex
	P            Point
	Level        int
	Friends      [][]*Node
	reverseLinks []*link
	id           NodeRef
}

type link struct {
	othernode  *Node
	otherlevel int
}

type Point []float32
type NodeRef uint64

func (a Point) Size() int {
	return len(a) * 4
}

func NewNode(p Point, level int, friends [][]*Node, id NodeRef) *Node {
	if friends != nil {
		return &Node{P: p, Level: level, Friends: friends, id: id}
	} else {
		return &Node{Level: 0, P: p, id: id}
	}
}

func (n *Node) GetFriends(level int) []*Node {
	if len(n.Friends) < level+1 {
		return make([]*Node, 0)
	}
	return n.Friends[level]
}

func (n *Node) FriendLevelCount() int {
	return len(n.Friends)
}

func (n *Node) FriendCountAtLevel(level int) int {
	return len(n.Friends[level])
}

func (n *Node) AddReverseLink(other *Node, level int) {
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

func (n *Node) UnlinkFromFriends() {
	for _, other := range n.reverseLinks {
		nodes := other.othernode.Friends[other.otherlevel]
		for j, x := range nodes {
			if x == n {
				// exclude me from array
				other.othernode.Friends[other.otherlevel] = append(other.othernode.Friends[other.otherlevel][:j], other.othernode.Friends[other.otherlevel][j+1:]...)
			}
		}
	}
}

func (n *Node) GetId() NodeRef {
	return n.id
}
