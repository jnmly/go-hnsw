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
	reverseLinks []*reverseLink
	id           uint
}

type reverseLink struct {
	othernode  *Node
	otherlevel int
}

type Point []float32

func (a Point) Size() int {
	return len(a) * 4
}

func NewNode(p Point, level int, friends [][]*Node, sequence *uint) *Node {
	// TODO: lock sequence here
	*sequence = *sequence + 1
	if friends != nil {
		return &Node{P: p, Level: level, Friends: friends, id: *sequence}
	} else {
		return &Node{Level: 0, P: p, id: *sequence}
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
		n.reverseLinks = make([]*reverseLink, 0)
	}

	n.reverseLinks = append(n.reverseLinks,
		&reverseLink{othernode: other,
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

func (n *Node) GetId() uint {
	return n.id
}
