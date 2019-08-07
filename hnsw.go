package hnsw

import (
	"math"
	"math/rand"
	"sync"

	"github.com/jnmly/go-hnsw/bitsetpool"
	"github.com/jnmly/go-hnsw/distqueue"
	"github.com/jnmly/go-hnsw/f32"
	"github.com/jnmly/go-hnsw/framework"
)

const (
	deluanayTypeSimple = iota
	deluanayTypeHeuristic
)

type Hnsw struct {
	sync.RWMutex
	framework.Hnsw

	DistFunc func([]float32, []float32) float32

	bitset *bitsetpool.BitsetPool
}

func (h *Hnsw) link(first *framework.Node, second uint64, level uint64) {
	maxL := h.M
	if level == 0 {
		maxL = h.M0
	}

	// check if we have allocated friends slices up to this level?
	if first.FriendLevelCount() < level+1 {
		first.AllocateFriendsUpTo(level, maxL)
	}

	// link with second node
	first.Friends[level].Nodes = append(first.Friends[level].Nodes, second) // HERE
	h.Nodes[second].AddReverseLink(first.GetNodeId(), level)

	if first.FriendCountAtLevel(level) > maxL {

		// too many links, deal with it

		switch h.DelaunayType {
		case deluanayTypeSimple:
			resultSet := &distqueue.DistQueue{Size: first.FriendCountAtLevel(level), ClosestLast: true}

			for _, n := range first.Friends[level].Nodes {
				resultSet.Push(n, h.DistFunc(first.P, h.Nodes[n].P))
			}
			for resultSet.Len() > maxL {
				resultSet.Pop()
			}
			// js: cleanup old reverse links
			for _, oldFriend := range first.Friends[level].Nodes {
				h.Nodes[oldFriend].RemoveReverseLink(first.GetNodeId(), level)
			}
			// FRIENDS ARE STORED IN DISTANCE ORDER, closest at index 0
			first.Friends[level].Nodes = first.Friends[level].Nodes[0:maxL]
			for i := maxL - 1; i >= 0; i-- {
				item := resultSet.Pop()
				first.Friends[level].Nodes[i] = item.Node
				h.Nodes[item.Node].AddReverseLink(first.GetNodeId(), level)
			}

		case deluanayTypeHeuristic:

			resultSet := &distqueue.DistQueue{Size: first.FriendCountAtLevel(level)}

			for _, n := range first.Friends[level].Nodes {
				resultSet.Push(n, h.DistFunc(first.P, h.Nodes[n].P))
			}
			h.getNeighborsByHeuristic(resultSet, maxL, false)

			// js: cleanup old reverse links
			for _, oldFriend := range first.Friends[level].Nodes {
				h.Nodes[oldFriend].RemoveReverseLink(first.GetNodeId(), level)
			}
			// FRIENDS ARE STORED IN DISTANCE ORDER, closest at index 0
			first.Friends[level].Nodes = first.Friends[level].Nodes[0:maxL]
			for i := uint64(0); i < maxL; i++ {
				item := resultSet.Pop()
				first.Friends[level].Nodes[i] = item.Node
				h.Nodes[item.Node].AddReverseLink(first.GetNodeId(), level)
			}
		}
	}
}

func (h *Hnsw) getNeighborsByHeuristic(resultSet *distqueue.DistQueue, M uint64, last bool) {
	var workSet *distqueue.DistQueue
	if resultSet.Len() <= M {
		return
	}
	tempList := &distqueue.DistQueue{Size: resultSet.Len()}
	result := make([]*distqueue.Item, 0, M)
	if last {
		tmpResultSet := &distqueue.DistQueue{Size: resultSet.Len()}
		for resultSet.Len() > 0 {
			tmpResultSet.PushItem(resultSet.Pop())
		}
		workSet = tmpResultSet
	} else {
		workSet = resultSet
	}
	for workSet.Len() > 0 {
		if uint64(len(result)) >= M {
			break
		}
		e := workSet.Pop()
		good := true
		for _, r := range result {
			if h.DistFunc(h.Nodes[r.Node].P, h.Nodes[e.Node].P) < e.D {
				good = false
				break
			}
		}
		if good {
			result = append(result, e)
		} else {
			tempList.PushItem(e)
		}
	}
	for uint64(len(result)) < M && tempList.Len() > 0 {
		result = append(result, tempList.Pop())
	}
	if !last {
		resultSet.Reset()
	}
	for _, item := range result {
		resultSet.PushItem(item)
	}
}

func New(M uint64, efConstruction uint64, first framework.Point) *Hnsw {

	h := Hnsw{}
	h.M = M

	// default values used in c++ implementation
	h.LevelMult = 1 / math.Log(float64(M))
	h.EfConstruction = efConstruction
	h.M0 = 2 * M
	h.DelaunayType = deluanayTypeHeuristic

	h.bitset = bitsetpool.New()

	//h.DistFunc = f32.L2Squared8AVX
	h.DistFunc = f32.L2Squared

	// add first point, it will be our enterpoint (index 0)
	h.Nodes = make(map[uint64]*framework.Node)
	firstnode := framework.NewNode(first, 0, 0)
	h.Nodes[0] = firstnode
	h.Enterpoint = uint64(0)

	h.CountLevel = make(map[uint64]uint64)
	h.CountLevel[0] = 1
	h.MaxLayer = 0
	h.Sequence = 1

	return &h
}

func (h *Hnsw) findBestEnterPoint(ep *distqueue.Item, q framework.Point, curlevel uint64, maxLayer uint64) *distqueue.Item {
	for level := maxLayer; level > curlevel; level-- {
		// js: start search at the least granular level
		for changed := true; changed; {
			changed = false
			for _, n := range h.Nodes[ep.Node].GetNodeFriends(level) {
				d := h.DistFunc(h.Nodes[n].P, q)
				if d < ep.D {
					ep = &distqueue.Item{Node: n, D: d}
					changed = true
				}
			}
		}
	}

	return ep
}

func (h *Hnsw) Add(q framework.Point) uint64 {
	h.Lock()
	defer h.Unlock()

	// generate random level
	curlevel := uint64(math.Floor(-math.Log(rand.Float64() * h.LevelMult)))

	currentMaxLayer := h.Nodes[h.Enterpoint].Level
	ep := &distqueue.Item{Node: h.Enterpoint, D: h.DistFunc(h.Nodes[h.Enterpoint].P, q)}

	indexForNewNode := h.Sequence
	h.Sequence++
	newNode := framework.NewNode(q, curlevel, indexForNewNode)
	h.CountLevel[curlevel]++

	// first pass, find another ep if curlevel < maxLayer
	ep = h.findBestEnterPoint(ep, q, curlevel, currentMaxLayer)

	// second pass, ef = efConstruction
	// loop through every level from the new nodes level down to level 0
	// create new connections in every layer
	for level := min(curlevel, currentMaxLayer); level < math.MaxUint64; level-- { // note: level intentionally overflows/wraps here

		resultSet := &distqueue.DistQueue{ClosestLast: true}
		h.searchAtLayer(q, resultSet, h.EfConstruction, ep, level)
		switch h.DelaunayType {
		case deluanayTypeSimple:
			// shrink resultSet to M closest elements (the simple heuristic)
			for resultSet.Len() > h.M {
				resultSet.Pop()
			}
		case deluanayTypeHeuristic:
			h.getNeighborsByHeuristic(resultSet, h.M, true)
		}
		newNode.AllocateFriendsUpTo(level, h.M) // js: potentially only needs to alloc this level
		newNode.Friends[level].Nodes = make([]uint64, resultSet.Len())
		for i := resultSet.Len() - 1; i < math.MaxUint64; i-- { // note: i intentionally overflows/wraps here
			item := resultSet.Pop()
			// store in order, closest at index 0
			newNode.Friends[level].Nodes[i] = item.Node // HERE
			h.Nodes[item.Node].AddReverseLink(indexForNewNode, level)
		}
	}

	// Add it and increase slice length if neccessary
	h.Nodes[indexForNewNode] = newNode

	// now add connections to newNode from newNodes neighbours (makes it visible in the graph)
	for level := min(curlevel, currentMaxLayer); level < math.MaxUint64; level-- { // note: level intentionally overflows/wraps here
		for _, n := range newNode.Friends[level].Nodes {
			h.link(h.Nodes[n], indexForNewNode, level)
		}
	}

	if curlevel > h.MaxLayer {
		h.MaxLayer = curlevel
		h.Enterpoint = indexForNewNode
	}

	return indexForNewNode
}

func (h *Hnsw) Remove(indexToRemove uint64) {
	h.Lock()
	defer h.Unlock()

	hn := h.Nodes[indexToRemove]
	delete(h.Nodes, indexToRemove)

	hn.UnlinkFromFriends(h.Nodes)

	h.CountLevel[hn.Level]--

	// Re-assign enterpoint
	if h.Enterpoint == indexToRemove {
		for layer := h.MaxLayer; layer < math.MaxUint64; layer-- { //note: level intentionally overflows/wraps here
			for i, nn := range h.Nodes {
				if nn.Level == layer {
					h.Enterpoint = i
					break
				}
			}
		}
	}

	// Delete unnecessary layers
	for layer := h.MaxLayer; layer < math.MaxUint64; layer-- { //note: level intentionally overflows/wraps here
		if h.CountLevel[layer] == 0 {
			h.MaxLayer--
		} else {
			break
		}
	}

	if h.Enterpoint == indexToRemove {
		panic("failed to reassign enterpoint")
	}
}

func (h *Hnsw) searchAtLayer(q framework.Point, resultSet *distqueue.DistQueue, efConstruction uint64, ep *distqueue.Item, level uint64) {
	var pool, visited = h.bitset.Get()

	candidates := &distqueue.DistQueue{Size: efConstruction * 3}

	visited.Set(uint(ep.Node))
	candidates.Push(ep.Node, ep.D)

	resultSet.Push(ep.Node, ep.D)

	for candidates.Len() > 0 {
		_, lowerBound := resultSet.Top() // worst distance so far
		c := candidates.Pop()

		if c.D > lowerBound {
			// since candidates is sorted, it wont get any better...
			break
		}

		if h.Nodes[c.Node].FriendLevelCount() >= level+1 {
			friends := h.Nodes[c.Node].Friends[level].Nodes
			for _, n := range friends {
				if !visited.Test(uint(n)) {
					visited.Set(uint(n))
					d := h.DistFunc(q, h.Nodes[n].P)
					_, topD := resultSet.Top()
					if resultSet.Len() < efConstruction {
						item := resultSet.Push(n, d)
						candidates.PushItem(item)
					} else if topD > d {
						// keep length of resultSet to max efConstruction
						item := resultSet.PopAndPush(n, d)
						candidates.PushItem(item)
					}
				}
			}
		}
	}
	h.bitset.Free(pool)
}

func (h *Hnsw) Search(q framework.Point, ef uint64, K uint64) *distqueue.DistQueue {
	h.RLock()
	currentMaxLayer := h.MaxLayer
	ep := &distqueue.Item{Node: h.Enterpoint, D: h.DistFunc(h.Nodes[h.Enterpoint].P, q)}

	resultSet := &distqueue.DistQueue{Size: ef + 1, ClosestLast: true}

	// first pass, find best ep
	ep = h.findBestEnterPoint(ep, q, 0, currentMaxLayer)

	h.searchAtLayer(q, resultSet, ef, ep, 0)
	h.RUnlock()

	for resultSet.Len() > K {
		resultSet.Pop()
	}
	return resultSet
}
