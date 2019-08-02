package framework

type Point []float32

func NewNode(p Point, level uint64, id uint64) *Node {
	n := &Node{}
	n.P = p
	n.Level = level
	n.Friends = make(map[uint64]*LinkList)
	n.ReverseFriends = make(map[uint64]*LinkMap)
	n.Id = id
	return n
}

func (n *Node) AllocateFriendsUpTo(level uint64, capacity uint64) {
	for i := n.FriendLevelCount(); i <= level; i++ {
		if n.Friends[i] == nil {
			n.Friends[i] = &LinkList{Nodes: make([]uint64, 0, capacity)}
		}
	}
}

func (n *Node) GetNodeFriends(level uint64) []uint64 {
	if uint64(len(n.Friends)) < level+1 {
		return make([]uint64, 0)
	}
	return n.Friends[level].Nodes
}

func (n *Node) FriendLevelCount() uint64 {
	high := uint64(0)
	for k, _ := range n.Friends {
		// NOTE: should we check if len(x.Nodes) > 0 too? or removal might have cleared this level
		if k > high {
			high = k
		}
	}
	return high
}

func (n *Node) FriendCountAtLevel(level uint64) uint64 {
	return uint64(len(n.Friends[level].Nodes))
}

func (n *Node) AddReverseLink(other uint64, level uint64) {
	if n.ReverseFriends[level] == nil {
		n.ReverseFriends[level] = &LinkMap{
			Nodes: make(map[uint64]bool),
		}
	}
	n.ReverseFriends[level].Nodes[other] = true
}

func (n *Node) UnlinkFromFriends(allnodes map[uint64]*Node) {
	for level, m := range n.ReverseFriends {
		for node, _ := range m.Nodes {
			xother := allnodes[node]
			if xother == nil {
				continue
			}
			Nodes := xother.Friends[level]
			for j, x := range Nodes.Nodes {
				if x == n.GetNodeId() {
					// exclude me from array
					xother.Friends[level].Nodes = append(xother.Friends[level].Nodes[:j], xother.Friends[level].Nodes[j+1:]...)
				}
			}
		}
	}
}

func (n *Node) GetNodeId() uint64 {
	return n.Id
}
