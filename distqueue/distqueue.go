package distqueue

type Item struct {
	Node uint64
	D    float32
}

type DistQueue struct {
	initiated   bool
	items       []*Item
	Size        uint64
	ClosestLast bool
}

func (pq *DistQueue) Init() *DistQueue {
	pq.items = make([]*Item, 1, pq.Size+1)
	pq.items[0] = nil // Heap queue first element should always be nil
	pq.initiated = true
	return pq
}

func (pq *DistQueue) Reset() {
	pq.items = pq.items[0:1]
}

// Push the value item into the priority queue with provided priority.
func (pq *DistQueue) Push(id uint64, d float32) *Item {
	if !pq.initiated {
		pq.Init()
	}
	item := &Item{Node: id, D: d}
	pq.items = append(pq.items, item)
	pq.swim(len(pq.items) - 1)
	return item
}

func (pq *DistQueue) PushItem(item *Item) {
	if !pq.initiated {
		pq.Init()
	}
	pq.items = append(pq.items, item)
	pq.swim(len(pq.items) - 1)
}

func (pq *DistQueue) Pop() *Item {
	if len(pq.items) <= 1 {
		return nil
	}
	var max = pq.items[1]
	pq.items[1], pq.items[pq.Len()] = pq.items[pq.Len()], pq.items[1]
	pq.items = pq.items[0:pq.Len()]
	pq.sink(1)
	return max
}

// PopAndPush pops the top element and adds a new to the heap in one operation which is faster than two seperate calls to Pop and Push
func (pq *DistQueue) PopAndPush(id uint64, d float32) *Item {
	if !pq.initiated {
		pq.Init()
	}
	item := &Item{Node: id, D: d}
	pq.items[1] = item
	pq.sink(1)
	return item
}

func (pq *DistQueue) Top() (uint64, float32) {
	if len(pq.items) <= 1 {
		return 0, 0
	}
	return pq.items[1].Node, pq.items[1].D
}

func (pq *DistQueue) Head() (uint64, float32) {
	if len(pq.items) <= 1 {
		return 0, 0
	}
	return pq.items[1].Node, pq.items[1].D
}

func (pq *DistQueue) Len() uint64 {
	return uint64(len(pq.items) - 1)
}

func (pq *DistQueue) Empty() bool {
	return len(pq.items) == 1
}

func (pq *DistQueue) swim(k int) {
	for k > 1 && pq.compare(pq.items[k/2].D, pq.items[k].D) {
		pq.items[k], pq.items[k/2] = pq.items[k/2], pq.items[k]
		k = k / 2
	}
}

func (pq *DistQueue) sink(k uint64) {
	for 2*k <= pq.Len() {
		var j = 2 * k
		if j < pq.Len() && pq.compare(pq.items[j].D, pq.items[j+1].D) {
			j++
		}
		if !pq.compare(pq.items[k].D, pq.items[j].D) {
			break
		}
		pq.items[k], pq.items[j] = pq.items[j], pq.items[k]
		k = j
	}
}

func (pq *DistQueue) compare(a float32, b float32) bool {
	if pq.ClosestLast {
		return a < b
	} else {
		return a > b
	}
}
