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
	ReverseLinks []*reverseLink
	id           uint
}

type reverseLink struct {
	Othernode  *Node
	Otherlevel int
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
	if n.ReverseLinks == nil {
		n.ReverseLinks = make([]*reverseLink, 0)
	}

	n.ReverseLinks = append(n.ReverseLinks,
		&reverseLink{Othernode: other,
			Otherlevel: level,
		},
	)
}

func (n *Node) UnlinkFromFriends() {
	for _, other := range n.ReverseLinks {
		nodes := other.Othernode.Friends[other.Otherlevel]
		for j, x := range nodes {
			if x == n {
				// exclude me from array
				other.Othernode.Friends[other.Otherlevel] = append(other.Othernode.Friends[other.Otherlevel][:j], other.Othernode.Friends[other.Otherlevel][j+1:]...)
			}
		}
	}
}

func (n *Node) GetId() uint {
	return n.id
}
