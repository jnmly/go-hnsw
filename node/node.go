package node

import (
	//"fmt"
	"sync"
)

type Node struct {
	sync.RWMutex
	P            Point
	Level        uint64
	Friends      map[uint64]*LinkList
	reverseLinks map[uint64]*linkMap
	id           NodeRef
}

type LinkList struct {
	Nodes []NodeRef
}

type linkMap struct {
	Nodes map[NodeRef]bool
}

type Point []float32
type NodeRef uint64

func (a Point) Size() int {
	return len(a) * 4
}

func NewNode(p Point, level uint64, id NodeRef) *Node {
	return &Node{
		P:            p,
		Level:        level,
		Friends:      make(map[uint64]*LinkList),
		reverseLinks: make(map[uint64]*linkMap),
		id:           id,
	}
}

func (n *Node) AllocateFriendsUpTo(level uint64, capacity uint64) {
	for i := n.FriendLevelCount(); i <= level; i++ {
		if n.Friends[i] == nil {
			n.Friends[i] = &LinkList{Nodes: make([]NodeRef, 0, capacity)}
		}
	}
}

func (n *Node) GetFriends(level uint64) []NodeRef {
	if uint64(len(n.Friends)) < level+1 {
		return make([]NodeRef, 0)
	}
	return n.Friends[level].Nodes
}

func (n *Node) FriendLevelCount() uint64 {
	high := uint64(0)
	for k, _ := range n.Friends {
		if k > high {
			high = k
		}
	}
	return high
}

func (n *Node) FriendCountAtLevel(level uint64) uint64 {
	return uint64(len(n.Friends[level].Nodes))
}

func (n *Node) AddReverseLink(other NodeRef, level uint64) {
	if n.reverseLinks[level] == nil {
		n.reverseLinks[level] = &linkMap{
			Nodes: make(map[NodeRef]bool),
		}
	}
	n.reverseLinks[level].Nodes[other] = true
}

func (n *Node) UnlinkFromFriends(allnodes map[NodeRef]*Node) {
	for level, m := range n.reverseLinks {
		for node, _ := range m.Nodes {
			xother := allnodes[node]
			if xother == nil {
				continue
			}
			Nodes := xother.Friends[level]
			for j, x := range Nodes.Nodes {
				if x == n.GetId() {
					// exclude me from array
					xother.Friends[level].Nodes = append(xother.Friends[level].Nodes[:j], xother.Friends[level].Nodes[j+1:]...)
				}
			}
		}
	}
}

func (n *Node) GetId() NodeRef {
	return n.id
}
