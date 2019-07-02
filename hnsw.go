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

type Hnsw struct {
	sync.RWMutex
	M              int
	M0             int
	efConstruction int
	linkMode       int
	DelaunayType   int

	DistFunc func([]float32, []float32) float32

	nodes []node.Node

	bitset *bitsetpool.BitsetPool

	LevelMult  float64
	maxLayer   int
	enterpoint *node.Node
}

func (h *Hnsw) Link(first, second uint32, level int) {

	maxL := h.M
	if level == 0 {
		maxL = h.M0
	}

	h.RLock()
	this := &h.nodes[first]
	h.RUnlock()

	this.Lock()

	// check if we have allocated friends slices up to this level?
	if len(this.Friends) < level+1 {
		for j := len(this.Friends); j <= level; j++ {
			// allocate new list with 0 elements but capacity maxL
			elem := make([]*node.Node, 0, maxL)
			this.Friends = append(this.Friends, elem)
		}
		// now grow it by one and add the first connection for this layer
		this.Friends[level] = this.Friends[level][0:1]
		this.Friends[level][0] = &h.nodes[second]

		//h.usesOfId[second] = &levelIndex{
		//	index: 0,
		//	level: level,
		//}

	} else {
		// we did have some already... this will allocate more space if it overflows maxL
		this.Friends[level] = append(this.Friends[level], &h.nodes[second])

		//h.usesOfId[second] = &levelIndex{
		//	index: len(node.Friends[level]) - 1,
		//	level: level,
		//}
	}

	l := len(this.Friends[level])

	if l > maxL {

		// too many links, deal with it

		switch h.DelaunayType {
		case 0:
			resultSet := &distqueue.DistQueueClosestLast{Size: len(this.Friends[level])}

			for _, n := range this.Friends[level] {
				resultSet.Push(n, h.DistFunc(this.P, n.P))
			}
			for resultSet.Len() > maxL {
				resultSet.Pop()
			}
			// FRIENDS ARE STORED IN DISTANCE ORDER, closest at index 0
			this.Friends[level] = this.Friends[level][0:maxL]
			for i := maxL - 1; i >= 0; i-- {
				item := resultSet.Pop()
				this.Friends[level][i] = item.ID
			}

		case 1:

			resultSet := &distqueue.DistQueueClosestFirst{Size: len(this.Friends[level])}

			for _, n := range this.Friends[level] {
				resultSet.Push(n, h.DistFunc(this.P, n.P))
			}
			h.getNeighborsByHeuristicClosestFirst(resultSet, maxL)

			// FRIENDS ARE STORED IN DISTANCE ORDER, closest at index 0
			this.Friends[level] = this.Friends[level][0:maxL]
			for i := 0; i < maxL; i++ {
				item := resultSet.Pop()
				this.Friends[level][i] = item.ID
			}
		}
	}
	this.Unlock()
}

func (h *Hnsw) getNeighborsByHeuristicClosestLast(resultSet1 *distqueue.DistQueueClosestLast, M int) {
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
		if len(result) >= M {
			break
		}
		e := resultSet.Pop()
		good := true
		for _, r := range result {
			if h.DistFunc(r.ID.P, e.ID.P) < e.D {
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
	for len(result) < M && tempList.Len() > 0 {
		result = append(result, tempList.Pop())
	}
	for _, item := range result {
		resultSet1.PushItem(item)
	}
}

func (h *Hnsw) getNeighborsByHeuristicClosestFirst(resultSet *distqueue.DistQueueClosestFirst, M int) {
	if resultSet.Len() <= M {
		return
	}
	tempList := &distqueue.DistQueueClosestFirst{Size: resultSet.Len()}
	result := make([]*distqueue.Item, 0, M)
	for resultSet.Len() > 0 {
		if len(result) >= M {
			break
		}
		e := resultSet.Pop()
		good := true
		for _, r := range result {
			if h.DistFunc(r.ID.P, e.ID.P) < e.D {
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
	for len(result) < M && tempList.Len() > 0 {
		result = append(result, tempList.Pop())
	}
	resultSet.Reset()

	for _, item := range result {
		resultSet.PushItem(item)
	}
}

func New(M int, efConstruction int, first node.Point) *Hnsw {

	h := Hnsw{}
	h.M = M
	// default values used in c++ implementation
	h.LevelMult = 1 / math.Log(float64(M))
	h.efConstruction = efConstruction
	h.M0 = 2 * M
	h.DelaunayType = 1

	h.bitset = bitsetpool.New()

	h.DistFunc = f32.L2Squared8AVX

	// add first point, it will be our enterpoint (index 0)
	h.nodes = []node.Node{node.Node{Level: 0, P: first}}
	h.enterpoint = &h.nodes[0]

	return &h
}

func (h *Hnsw) Stats() string {
	s := "HNSW Index\n"
	s = s + fmt.Sprintf("M: %v, efConstruction: %v\n", h.M, h.efConstruction)
	s = s + fmt.Sprintf("DelaunayType: %v\n", h.DelaunayType)
	s = s + fmt.Sprintf("Number of nodes: %v\n", len(h.nodes))
	s = s + fmt.Sprintf("Max layer: %v\n", h.maxLayer)
	memoryUseData := 0
	memoryUseIndex := 0
	levCount := make([]int, h.maxLayer+1)
	conns := make([]int, h.maxLayer+1)
	connsC := make([]int, h.maxLayer+1)
	for i := range h.nodes {
		levCount[h.nodes[i].Level]++
		for j := 0; j <= h.nodes[i].Level; j++ {
			if len(h.nodes[i].Friends) > j {
				l := len(h.nodes[i].Friends[j])
				conns[j] += l
				connsC[j]++
			}
		}
		memoryUseData += h.nodes[i].P.Size()
		memoryUseIndex += h.nodes[i].Level*h.M*4 + h.M0*4
	}
	for i := range levCount {
		avg := conns[i] / max(1, connsC[i])
		s = s + fmt.Sprintf("Level %v: %v nodes, average number of connections %v\n", i, levCount[i], avg)
	}
	s = s + fmt.Sprintf("Memory use for data: %v (%v bytes / point)\n", memoryUseData, memoryUseData/len(h.nodes))
	s = s + fmt.Sprintf("Memory use for index: %v (avg %v bytes / point)\n", memoryUseIndex, memoryUseIndex/len(h.nodes))
	return s
}

func (h *Hnsw) Print() string {
	buf := strings.Builder{}

	for i, n := range h.nodes {
		buf.WriteString(fmt.Sprintf("node %d\n", i))
		for j := range n.Friends {
			arr := n.Friends[j]
			for k := range arr {
				buf.WriteString(fmt.Sprintf("     level %d friend %d = %d\n", j, k, n.Friends[j][k].Myid))
			}
		}
		buf.WriteString("\n\n\n")
	}

	return buf.String()
}

func (h *Hnsw) Grow(size int) {
	if size+1 <= len(h.nodes) {
		return
	}
	newNodes := make([]node.Node, len(h.nodes), size+1)
	copy(newNodes, h.nodes)
	h.nodes = newNodes

}

func (h *Hnsw) Add(q node.Point, id uint32) {

	if id == 0 {
		panic("Id 0 is reserved, use ID:s starting from 1 when building index")
	}

	// generate random level
	curlevel := int(math.Floor(-math.Log(rand.Float64() * h.LevelMult)))

	currentMaxLayer := h.enterpoint.Level
	ep := &distqueue.Item{ID: h.enterpoint, D: h.DistFunc(h.enterpoint.P, q)}

	// assume Grow has been called in advance
	newID := id
	newNode := node.Node{Myid: id, P: q, Level: curlevel, Friends: make([][]*node.Node, min(curlevel, currentMaxLayer)+1)}

	// first pass, find another ep if curlevel < maxLayer
	for level := currentMaxLayer; level > curlevel; level-- {
		changed := true
		for changed {
			changed = false
			for _, n := range ep.ID.GetFriends(level) {
				d := h.DistFunc(n.P, q)
				if d < ep.D {
					ep = &distqueue.Item{ID: n, D: d}
					changed = true
				}
			}
		}
	}

	// second pass, ef = efConstruction
	// loop through every level from the new nodes level down to level 0
	// create new connections in every layer
	for level := min(curlevel, currentMaxLayer); level >= 0; level-- {

		resultSet := &distqueue.DistQueueClosestLast{}
		h.searchAtLayer(q, resultSet, h.efConstruction, ep, level)
		switch h.DelaunayType {
		case 0:
			// shrink resultSet to M closest elements (the simple heuristic)
			for resultSet.Len() > h.M {
				resultSet.Pop()
			}
		case 1:
			h.getNeighborsByHeuristicClosestLast(resultSet, h.M)
		}
		newNode.Friends[level] = make([]*node.Node, resultSet.Len())
		for i := resultSet.Len() - 1; i >= 0; i-- {
			item := resultSet.Pop()
			// store in order, closest at index 0
			newNode.Friends[level][i] = item.ID
		}
	}

	h.Lock()
	// Add it and increase slice length if neccessary
	if len(h.nodes) < int(newID)+1 {
		h.nodes = h.nodes[0 : newID+1]
	}
	h.nodes[newID] = newNode
	h.Unlock()

	// now add connections to newNode from newNodes neighbours (makes it visible in the graph)
	for level := min(curlevel, currentMaxLayer); level >= 0; level-- {
		for _, n := range newNode.Friends[level] {
			h.Link(n.Myid, newID, level)
		}
	}

	h.Lock()
	if curlevel > h.maxLayer {
		h.maxLayer = curlevel
		h.enterpoint = &newNode
	}
	h.Unlock()
}

func (h *Hnsw) Remove(id uint32) {
	base := h.nodes[:id]
	other := h.nodes[id+1:]
	h.nodes = append(base, other...)
}

func (h *Hnsw) searchAtLayer(q node.Point, resultSet *distqueue.DistQueueClosestLast, efConstruction int, ep *distqueue.Item, level int) {

	var pool, visited = h.bitset.Get()
	//visited := make(map[uint32]bool)

	candidates := &distqueue.DistQueueClosestFirst{Size: efConstruction * 3}

	visited.Set(uint(ep.ID.Myid))
	//visited[ep.ID] = true
	candidates.Push(ep.ID, ep.D)

	resultSet.Push(ep.ID, ep.D)

	for candidates.Len() > 0 {
		_, lowerBound := resultSet.Top() // worst distance so far
		c := candidates.Pop()

		if c.D > lowerBound {
			// since candidates is sorted, it wont get any better...
			break
		}

		if len(c.ID.Friends) >= level+1 {
			Friends := c.ID.Friends[level]
			for _, n := range Friends {
				if !visited.Test(uint(n.Myid)) {
					visited.Set(uint(n.Myid))
					d := h.DistFunc(q, n.P)
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

// SearchBrute returns the true K nearest neigbours to search point q
func (h *Hnsw) SearchBrute(q node.Point, K int) *distqueue.DistQueueClosestLast {
	resultSet := &distqueue.DistQueueClosestLast{Size: K}
	for i := 1; i < len(h.nodes); i++ {
		d := h.DistFunc(h.nodes[i].P, q)
		if resultSet.Len() < K {
			resultSet.Push(&h.nodes[i], d)
			continue
		}
		_, topD := resultSet.Head()
		if d < topD {
			resultSet.PopAndPush(&h.nodes[i], d)
			continue
		}
	}
	return resultSet
}

func (h *Hnsw) Search(q node.Point, ef int, K int) *distqueue.DistQueueClosestLast {

	h.RLock()
	currentMaxLayer := h.maxLayer
	ep := &distqueue.Item{ID: h.enterpoint, D: h.DistFunc(h.enterpoint.P, q)}
	h.RUnlock()

	resultSet := &distqueue.DistQueueClosestLast{Size: ef + 1}
	// first pass, find best ep
	for level := currentMaxLayer; level > 0; level-- {
		changed := true
		for changed {
			changed = false
			for _, n := range ep.ID.GetFriends(level) {
				d := h.DistFunc(n.P, q)
				if d < ep.D {
					ep.ID, ep.D = n, d
					changed = true
				}
			}
		}
	}
	h.searchAtLayer(q, resultSet, ef, ep, 0)

	for resultSet.Len() > K {
		resultSet.Pop()
	}
	return resultSet
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
