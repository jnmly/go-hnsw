package hnsw

import (
	"github.com/jnmly/go-hnsw/framework"
)

func (h *Hnsw) toFramework() *framework.Hnsw {
	countlevels := make([]*framework.LevelCount, 0, len(h.countLevel))
	for k, v := range h.countLevel {
		countlevels = append(countlevels, &framework.LevelCount{
			Level: int64(k),
			Count: int64(v),
		})
	}

	nodes := make([]*framework.Node, 0, len(h.nodes))
	for _, n := range h.nodes {
		friends := make([]*framework.Link, 0, len(n.Friends)*h.M)
		reverse := make([]*framework.Link, 0, len(n.Friends)*h.M)

		for level, list := range n.Friends {
			for _, othernode := range list {
				friends = append(friends, &framework.Link{
					Level: int64(level),
					Id:    uint64(othernode.GetId()),
				})
			}
		}

		for _, link := range n.ReverseLinks {
			reverse = append(reverse, &framework.Link{
				Level: int64(link.Otherlevel),
				Id:    uint64(link.Othernode.GetId()),
			})
		}

		nodes = append(nodes, &framework.Node{
			Id:      uint64(n.GetId()),
			Vector:  n.P,
			Level:   int64(n.Level),
			Friends: nil,
			Reverse: nil,
		})
	}

	f := &framework.Hnsw{
		M:              int64(h.M),
		M0:             int64(h.M0),
		EfConstruction: int64(h.efConstruction),
		DelaunayType:   int64(h.DelaunayType),
		LevelMult:      float32(h.LevelMult),
		MaxLayer:       int64(h.maxLayer),
		Sequence:       int64(h.sequence),
		CountLevel:     countlevels,
		EnterPoint:     uint64(h.enterpoint.GetId()),
		Nodes:          nodes,
	}

	return f
}
