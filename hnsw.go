package hnsw

import (
	"fmt"
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
	//fmt.Printf("entered link\n")
	//defer fmt.Printf("left link\n")

	maxL := h.M
	if level == 0 {
		maxL = h.M0
	}

	//first.Lock()

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

			// HERE

		case deluanayTypeHeuristic:

			resultSet := &distqueue.DistQueue{Size: first.FriendCountAtLevel(level)}

			for _, n := range first.Friends[level].Nodes {
				resultSet.Push(n, h.DistFunc(first.P, h.Nodes[n].P))
			}
			h.getNeighborsByHeuristicClosestFirst(resultSet, maxL)

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

			// HERE
		}
	}
	//first.Unlock()
}

func (h *Hnsw) getNeighborsByHeuristicClosestLast(resultSet1 *distqueue.DistQueue, M uint64) {
	//fmt.Printf("entered getNeighborsByHeuristicClosestLast\n")
	//defer fmt.Printf("left getNeighborsByHeuristicClosestLast\n")
	if resultSet1.Len() <= M {
		return
	}
	resultSet := &distqueue.DistQueue{Size: resultSet1.Len()}
	tempList := &distqueue.DistQueue{Size: resultSet1.Len()}
	result := make([]*distqueue.Item, 0, M)
	for resultSet1.Len() > 0 {
		resultSet.PushItem(resultSet1.Pop())
	}
	for resultSet.Len() > 0 {
		if uint64(len(result)) >= M {
			break
		}
		e := resultSet.Pop()
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
	for _, item := range result {
		resultSet1.PushItem(item)
	}
}

func (h *Hnsw) getNeighborsByHeuristicClosestFirst(resultSet *distqueue.DistQueue, M uint64) {
	//fmt.Printf("entered getNeighborsByHeuristicClosestFirst\n")
	//defer fmt.Printf("left getNeighborsByHeuristicClosestFirst\n")
	if resultSet.Len() <= M {
		return
	}
	tempList := &distqueue.DistQueue{Size: resultSet.Len()}
	result := make([]*distqueue.Item, 0, M)
	for resultSet.Len() > 0 {
		if uint64(len(result)) >= M {
			break
		}
		e := resultSet.Pop()
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
	resultSet.Reset()

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

	// TODO: lock
	h.CountLevel = make(map[uint64]uint64)
	h.CountLevel[0] = 1
	h.MaxLayer = 0
	h.Sequence = 1

	return &h
}

func (h *Hnsw) Stats() string {
	h.RLock()
	defer h.RUnlock()

	s := "HNSW Index\n"
	s = s + fmt.Sprintf("M: %v, efConstruction: %v\n", h.M, h.EfConstruction)
	s = s + fmt.Sprintf("DelaunayType: %v\n", h.DelaunayType)
	s = s + fmt.Sprintf("Number of nodes: %v\n", len(h.Nodes))
	s = s + fmt.Sprintf("Max layer: %v\n", h.MaxLayer)
	memoryUseData := 0
	memoryUseIndex := uint64(0)
	levCount := make([]uint64, h.MaxLayer+1)
	conns := make([]uint64, h.MaxLayer+1)
	connsC := make([]uint64, h.MaxLayer+1)
	for i := range h.Nodes {
		levCount[h.Nodes[i].Level]++
		for j := uint64(0); j <= h.Nodes[i].Level; j++ {
			if h.Nodes[i].FriendLevelCount() > j {
				l := len(h.Nodes[i].Friends[j].Nodes)
				conns[j] += uint64(l)
				connsC[j]++
			}
		}
		memoryUseData += len(h.Nodes[i].P) * 4
		memoryUseIndex += h.Nodes[i].Level*h.M*4 + h.M0*4
	}
	for i := range levCount {
		avg := conns[i] / max(1, connsC[i])
		s = s + fmt.Sprintf("Level %v: %v (%d) nodes, average number of connections %v\n", i, levCount[uint64(i)], h.CountLevel[uint64(i)], avg)
	}
	s = s + fmt.Sprintf("Memory use for data: %v (%v bytes / point)\n", memoryUseData, memoryUseData/len(h.Nodes))
	s = s + fmt.Sprintf("Memory use for index: %v (avg %v bytes / point)\n", memoryUseIndex, memoryUseIndex/uint64(len(h.Nodes)))
	return s
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
	//fmt.Printf("entered Add\n")
	//defer fmt.Printf("left Add\n")
	h.Lock()
	defer h.Unlock()

	// generate random level
	curlevel := uint64(math.Floor(-math.Log(rand.Float64() * h.LevelMult)))

	currentMaxLayer := h.Nodes[h.Enterpoint].Level
	ep := &distqueue.Item{Node: h.Enterpoint, D: h.DistFunc(h.Nodes[h.Enterpoint].P, q)}

	//newNode := &node.Node{P: q, Level: curlevel, Friends: make([][]*node.Node, min(curlevel, currentMaxLayer)+1))}
	indexForNewNode := h.Sequence
	h.Sequence++
	newNode := framework.NewNode(q, curlevel, indexForNewNode)
	// TODO: lock
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
			h.getNeighborsByHeuristicClosestLast(resultSet, h.M)
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

	//h.Lock()
	// Add it and increase slice length if neccessary
	h.Nodes[indexForNewNode] = newNode
	//h.Unlock()

	// now add connections to newNode from newNodes neighbours (makes it visible in the graph)
	for level := min(curlevel, currentMaxLayer); level < math.MaxUint64; level-- { // note: level intentionally overflows/wraps here
		for _, n := range newNode.Friends[level].Nodes {
			h.link(h.Nodes[n], indexForNewNode, level)
		}
	}

	//h.Lock()
	if curlevel > h.MaxLayer {
		h.MaxLayer = curlevel
		h.Enterpoint = indexForNewNode
	}
	//h.Unlock()

	return indexForNewNode
}

func (h *Hnsw) Remove(indexToRemove uint64) {
	//fmt.Printf("entered Remove\n")
	//defer fmt.Printf("left Remove\n")

	h.Lock()
	defer h.Unlock()

	hn := h.Nodes[indexToRemove]
	delete(h.Nodes, indexToRemove)
	//fmt.Printf("Removing id=%d\n", indexToRemove)

	// TODO: fix speedup, no need for array here

	hn.UnlinkFromFriends(h.Nodes)

	// TODO: lock
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

	//fmt.Printf("entered searchAtLayer\n")
	//defer fmt.Printf("left searchAtLayer\n")

	var pool, visited = h.bitset.Get()
	//visited := make(map[uint32]bool)

	candidates := &distqueue.DistQueue{Size: efConstruction * 3}

	visited.Set(uint(ep.Node))
	//visited[ep.Node] = true
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
	//fmt.Printf("entered Search\n")
	//defer fmt.Printf("left Search\n")

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

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
