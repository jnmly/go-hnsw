package node

import (
	//"fmt"
	"sync"
)

type Node struct {
	sync.RWMutex
	locked       bool
	P            Point
	Level        int
	Friends      [][]*Node
	Myid         uint32
	reverseLinks []*ReverseLink
}

type ReverseLink struct {
	othernode  *Node
	otherlevel int
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

func (n *Node) FriendLevelCount() int {
	return len(n.Friends)
}

func (n *Node) FriendCountAtLevel(level int) int {
	return len(n.Friends[level])
}

func (n *Node) AddReverseLink(other *Node, level int) {
	if n.reverseLinks == nil {
		n.reverseLinks = make([]*ReverseLink, 0)
	}

	//if n.Myid == 501 {
	//	fmt.Printf("rlink %d at level %d\n", other.Myid, level)
	//}
	n.reverseLinks = append(n.reverseLinks,
		&ReverseLink{othernode: other,
			otherlevel: level,
		},
	)
}

func (n *Node) UnlinkFromFriends() {
	//fmt.Printf("UNLINK %d\n", len(n.reverseLinks))
	for _, other := range n.reverseLinks {
		nodes := other.othernode.Friends[other.otherlevel]
		for j, x := range nodes {
			if x == n {
				// exclude me from array
				//fmt.Printf("unlinking node %d, level %d for %d\n", n.Myid, other.otherlevel, other.othernode.Myid)
				other.othernode.Friends[other.otherlevel] = append(other.othernode.Friends[other.otherlevel][:j], other.othernode.Friends[other.otherlevel][j+1:]...)
			}
		}
	}
}
