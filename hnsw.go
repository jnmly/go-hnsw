package hnsw

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"

	"github.com/jnmly/go-hnsw/bitsetpool"
	"github.com/jnmly/go-hnsw/distqueue"
	"github.com/jnmly/go-hnsw/f32"
	"github.com/jnmly/go-hnsw/node"
)

const (
	deluanayTypeSimple = iota
	deluanayTypeHeuristic
)

type Hnsw struct {
	sync.RWMutex
	M              uint64
	M0             uint64
	efConstruction uint64
	DelaunayType   uint64

	DistFunc func([]float32, []float32) float32

	nodes map[node.NodeRef]*node.Node // TODO: locking

	bitset *bitsetpool.BitsetPool

	LevelMult  float64
	maxLayer   uint64
	enterpoint node.NodeRef

	countLevel map[uint64]uint64
	sequence   node.NodeRef
}

func (h *Hnsw) Link(first *node.Node, second node.NodeRef, level uint64) {
	//fmt.Printf("entered Link\n")
	//defer fmt.Printf("left Link\n")

	maxL := h.M
	if level == 0 {
		maxL = h.M0
	}

	first.Lock()

	// check if we have allocated friends slices up to this level?
	if first.FriendLevelCount() < level+1 {
		for j := first.FriendLevelCount(); j <= level; j++ {
			// allocate new list with 0 elements but capacity maxL
			first.Friends = append(first.Friends, make([]node.NodeRef, 0, maxL))
		}
	}

	// link with second node
	first.Friends[level] = append(first.Friends[level], second) // HERE
	h.nodes[second].AddReverseLink(first.GetId(), level)

	if first.FriendCountAtLevel(level) > maxL {

		// too many links, deal with it

		switch h.DelaunayType {
		case deluanayTypeSimple:
			resultSet := &distqueue.DistQueueClosestLast{Size: first.FriendCountAtLevel(level)}

			for _, n := range first.Friends[level] {
				resultSet.Push(n, h.DistFunc(first.P, h.nodes[n].P))
			}
			for resultSet.Len() > maxL {
				resultSet.Pop()
			}
			// FRIENDS ARE STORED IN DISTANCE ORDER, closest at index 0
			first.Friends[level] = first.Friends[level][0:maxL]
			for i := maxL - 1; i >= 0; i-- {
				item := resultSet.Pop()
				first.Friends[level][i] = item.Node
				h.nodes[item.Node].AddReverseLink(first.GetId(), level) // really needed?
			}

			// HERE

		case deluanayTypeHeuristic:

			resultSet := &distqueue.DistQueueClosestFirst{Size: first.FriendCountAtLevel(level)}

			for _, n := range first.Friends[level] {
				resultSet.Push(n, h.DistFunc(first.P, h.nodes[n].P))
			}
			h.getNeighborsByHeuristicClosestFirst(resultSet, maxL)

			// FRIENDS ARE STORED IN DISTANCE ORDER, closest at index 0
			first.Friends[level] = first.Friends[level][0:maxL]
			for i := uint64(0); i < maxL; i++ {
				item := resultSet.Pop()
				first.Friends[level][i] = item.Node
				h.nodes[item.Node].AddReverseLink(first.GetId(), level) // really needed?
			}

			// HERE
		}
	}
	first.Unlock()
}

func (h *Hnsw) getNeighborsByHeuristicClosestLast(resultSet1 *distqueue.DistQueueClosestLast, M uint64) {
	//fmt.Printf("entered getNeighborsByHeuristicClosestLast\n")
	//defer fmt.Printf("left getNeighborsByHeuristicClosestLast\n")
	if resultSet1.Len() <= M {
		return
	}
	resultSet := &distqueue.DistQueueClosestFirst{Size: resultSet1.Len()}
	tempList := &distqueue.DistQueueClosestFirst{Size: resultSet1.Len()}
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
			if h.DistFunc(h.nodes[r.Node].P, h.nodes[e.Node].P) < e.D {
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

func (h *Hnsw) getNeighborsByHeuristicClosestFirst(resultSet *distqueue.DistQueueClosestFirst, M uint64) {
	//fmt.Printf("entered getNeighborsByHeuristicClosestFirst\n")
	//defer fmt.Printf("left getNeighborsByHeuristicClosestFirst\n")
	if resultSet.Len() <= M {
		return
	}
	tempList := &distqueue.DistQueueClosestFirst{Size: resultSet.Len()}
	result := make([]*distqueue.Item, 0, M)
	for resultSet.Len() > 0 {
		if uint64(len(result)) >= M {
			break
		}
		e := resultSet.Pop()
		good := true
		for _, r := range result {
			if h.DistFunc(h.nodes[r.Node].P, h.nodes[e.Node].P) < e.D {
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

func New(M uint64, efConstruction uint64, first node.Point) *Hnsw {

	h := Hnsw{}
	h.M = M
	// default values used in c++ implementation
	h.LevelMult = 1 / math.Log(float64(M))
	h.efConstruction = efConstruction
	h.M0 = 2 * M
	h.DelaunayType = deluanayTypeHeuristic

	h.bitset = bitsetpool.New()

	//h.DistFunc = f32.L2Squared8AVX
	h.DistFunc = f32.L2Squared

	// add first point, it will be our enterpoint (index 0)
	h.nodes = make(map[node.NodeRef]*node.Node)
	firstnode := node.NewNode(first, 0, nil, 0)
	h.nodes[0] = firstnode
	h.enterpoint = node.NodeRef(0)

	// TODO: lock
	h.countLevel = make(map[uint64]uint64)
	h.countLevel[0] = 1
	h.maxLayer = 0
	h.sequence = 1

	return &h
}

func (h *Hnsw) Stats() string {
	s := "HNSW Index\n"
	s = s + fmt.Sprintf("M: %v, efConstruction: %v\n", h.M, h.efConstruction)
	s = s + fmt.Sprintf("DelaunayType: %v\n", h.DelaunayType)
	s = s + fmt.Sprintf("Number of nodes: %v\n", len(h.nodes))
	s = s + fmt.Sprintf("Max layer: %v\n", h.maxLayer)
	memoryUseData := 0
	memoryUseIndex := uint64(0)
	levCount := make([]uint64, h.maxLayer+1)
	conns := make([]uint64, h.maxLayer+1)
	connsC := make([]uint64, h.maxLayer+1)
	for i := range h.nodes {
		levCount[h.nodes[i].Level]++
		for j := uint64(0); j <= h.nodes[i].Level; j++ {
			if h.nodes[i].FriendLevelCount() > j {
				l := len(h.nodes[i].Friends[j])
				conns[j] += uint64(l)
				connsC[j]++
			}
		}
		memoryUseData += h.nodes[i].P.Size()
		memoryUseIndex += h.nodes[i].Level*h.M*4 + h.M0*4
	}
	for i := range levCount {
		avg := conns[i] / max(1, connsC[i])
		s = s + fmt.Sprintf("Level %v: %v (%d) nodes, average number of connections %v\n", i, levCount[uint64(i)], h.countLevel[uint64(i)], avg)
	}
	s = s + fmt.Sprintf("Memory use for data: %v (%v bytes / point)\n", memoryUseData, memoryUseData/len(h.nodes))
	s = s + fmt.Sprintf("Memory use for index: %v (avg %v bytes / point)\n", memoryUseIndex, memoryUseIndex/uint64(len(h.nodes)))
	return s
}

func (h *Hnsw) Print() string {
	buf := strings.Builder{}

	buf.WriteString(fmt.Sprintf("enterpoint = %d %p\n", h.enterpoint, h.nodes[h.enterpoint]))

	for i, n := range h.nodes {
		buf.WriteString(fmt.Sprintf("node %d, level %d, addr %p\n", i, n.Level, n))
		for lvl, arr := range n.Friends {
			for friendindex, f := range arr {
				buf.WriteString(fmt.Sprintf("     level %d friend %d = %d\n", lvl, friendindex, f))
			}
		}
		buf.WriteString("\n\n\n")
	}

	return buf.String()
}

func (h *Hnsw) findBestEnterPoint(ep *distqueue.Item, q node.Point, curlevel uint64, maxLayer uint64) *distqueue.Item {
	for level := maxLayer; level > curlevel; level-- {
		// js: start search at the least granular level
		for changed := true; changed; {
			changed = false
			for _, n := range h.nodes[ep.Node].GetFriends(level) {
				d := h.DistFunc(h.nodes[n].P, q)
				if d < ep.D {
					ep = &distqueue.Item{Node: n, D: d}
					changed = true
				}
			}
		}
	}

	return ep
}

func (h *Hnsw) Add(q node.Point) *node.Node {
	//fmt.Printf("entered Add\n")
	//defer fmt.Printf("left Add\n")

	// generate random level
	curlevel := uint64(math.Floor(-math.Log(rand.Float64() * h.LevelMult)))

	currentMaxLayer := h.nodes[h.enterpoint].Level
	ep := &distqueue.Item{Node: h.enterpoint, D: h.DistFunc(h.nodes[h.enterpoint].P, q)}

	//newNode := &node.Node{P: q, Level: curlevel, Friends: make([][]*node.Node, min(curlevel, currentMaxLayer)+1))}
	indexForNewNode := h.sequence
	h.sequence++
	newNode := node.NewNode(q, curlevel, make([][]node.NodeRef, min(curlevel, currentMaxLayer)+1), indexForNewNode)
	// TODO: lock
	h.countLevel[curlevel]++

	// first pass, find another ep if curlevel < maxLayer
	ep = h.findBestEnterPoint(ep, q, curlevel, currentMaxLayer)

	// second pass, ef = efConstruction
	// loop through every level from the new nodes level down to level 0
	// create new connections in every layer
	for level := min(curlevel, currentMaxLayer); level < math.MaxUint64; level-- { // note: level intentionally overflows/wraps here

		resultSet := &distqueue.DistQueueClosestLast{}
		h.searchAtLayer(q, resultSet, h.efConstruction, ep, level)
		switch h.DelaunayType {
		case deluanayTypeSimple:
			// shrink resultSet to M closest elements (the simple heuristic)
			for resultSet.Len() > h.M {
				resultSet.Pop()
			}
		case deluanayTypeHeuristic:
			h.getNeighborsByHeuristicClosestLast(resultSet, h.M)
		}
		newNode.Friends[level] = make([]node.NodeRef, resultSet.Len())
		for i := resultSet.Len() - 1; i < math.MaxUint64; i-- { // note: i intentionally overflows/wraps here
			item := resultSet.Pop()
			// store in order, closest at index 0
			newNode.Friends[level][i] = item.Node // HERE
			h.nodes[item.Node].AddReverseLink(indexForNewNode, level)
		}
	}

	h.Lock()
	// Add it and increase slice length if neccessary
	h.nodes[node.NodeRef(indexForNewNode)] = newNode
	h.Unlock()

	// now add connections to newNode from newNodes neighbours (makes it visible in the graph)
	for level := min(curlevel, currentMaxLayer); level < math.MaxUint64; level-- { // note: level intentionally overflows/wraps here
		for _, n := range newNode.Friends[level] {
			h.Link(h.nodes[n], indexForNewNode, level)
		}
	}

	h.Lock()
	if curlevel > h.maxLayer {
		h.maxLayer = curlevel
		h.enterpoint = node.NodeRef(indexForNewNode)
	}
	h.Unlock()

	return newNode
}

func (h *Hnsw) Remove(indexToRemove node.NodeRef) {
	//fmt.Printf("entered Remove\n")
	//defer fmt.Printf("left Remove\n")

	h.Lock()
	hn := h.nodes[indexToRemove]
	delete(h.nodes, indexToRemove)
	h.Unlock()

	// TODO: fix speedup, no need for array here

	hn.UnlinkFromFriends(h.nodes)

	// TODO: lock
	h.countLevel[hn.Level]--

	// Re-assign enterpoint
	if h.enterpoint == indexToRemove {
		for layer := h.maxLayer; layer >= 0; layer-- {
			for i, nn := range h.nodes {
				if nn.Level == layer {
					h.enterpoint = i
					break
				}
			}
		}
	}

	// Delete unnecessary layers
	for layer := h.maxLayer; layer >= 0; layer-- {
		if h.countLevel[layer] == 0 {
			h.maxLayer--
		} else {
			break
		}
	}

	if h.enterpoint == indexToRemove {
		panic("failed to reassign enterpoint")
	}
}

func (h *Hnsw) searchAtLayer(q node.Point, resultSet *distqueue.DistQueueClosestLast, efConstruction uint64, ep *distqueue.Item, level uint64) {

	//fmt.Printf("entered searchAtLayer\n")
	//defer fmt.Printf("left searchAtLayer\n")

	var pool, visited = h.bitset.Get()
	//visited := make(map[uint32]bool)

	candidates := &distqueue.DistQueueClosestFirst{Size: efConstruction * 3}

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

		if h.nodes[c.Node].FriendLevelCount() >= level+1 {
			friends := h.nodes[c.Node].Friends[level]
			for _, n := range friends {
				if !visited.Test(uint(n)) {
					visited.Set(uint(n))
					d := h.DistFunc(q, h.nodes[n].P)
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

func (h *Hnsw) Search(q node.Point, ef uint64, K uint64) *distqueue.DistQueueClosestLast {
	//fmt.Printf("entered Search\n")
	//defer fmt.Printf("left Search\n")

	h.RLock()
	currentMaxLayer := h.maxLayer
	ep := &distqueue.Item{Node: h.enterpoint, D: h.DistFunc(h.nodes[h.enterpoint].P, q)}
	h.RUnlock()

	resultSet := &distqueue.DistQueueClosestLast{Size: ef + 1}
	// first pass, find best ep
	ep = h.findBestEnterPoint(ep, q, 0, currentMaxLayer)

	h.searchAtLayer(q, resultSet, ef, ep, 0)

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
