package node

import (
	"sync"
)

type Node struct {
	sync.RWMutex
	locked  bool
	P       Point
	Level   int
	Friends [][]*Node
	Myid    uint32
}

type Point []float32

func (a Point) Size() int {
	return len(a) * 4
}

func (n *Node) GetFriends(level int) []*Node {
	if len(n.Friends) < level+1 {
		return make([]*Node, 0)
	}
	return n.Friends[level]
}

func (n *Node) FriendCount() int {
	return len(n.Friends)
}

func (n *Node) FriendCountLevel(level int) int {
	return len(n.Friends[level])
}
