package distqueue

import (
	"math/rand"
	"testing"

	"github.com/jnmly/go-hnsw/node"
	"github.com/stretchr/testify/assert"
)

func TestQueue(t *testing.T) {

	pq := &DistQueueClosestFirst{}

	for i := 0; i < 10; i++ {
		pq.Push(
			&node.Node{},
			float32(rand.Float64()))
	}

	t.Log("Closest first, pop")
	ID, D := pq.Top()
	t.Logf("TOP before first top: %v %v", ID, D)
	var l float32 = 0.0
	for pq.Len() > 0 {
		item := pq.Pop()
		if item.D < l {
			t.Error("Incorrect order")
		}
		l = item.D
		t.Logf("%+v", item)
	}

	pq2 := &DistQueueClosestLast{}
	l = 1.0
	pq2.Init()
	pq2.Reserve(200) // try reserve
	for i := 0; i < 10; i++ {
		pq2.Push(&node.Node{}, float32(rand.Float64()))
	}
	t.Log("Closest last, pop")
	for !pq2.Empty() {
		item := pq2.Pop()
		if item.D > l {
			t.Error("Incorrect order")
		}
		l = item.D
		t.Logf("%+v", item)
	}
}

func TestKBest(t *testing.T) {

	pq := &DistQueueClosestFirst{}
	pq.Reserve(5) // reserve less than needed
	for i := 0; i < 20; i++ {
		pq.Push(&node.Node{}, rand.Float32())
	}

	// return K best matches, ordered as best first
	t.Log("closest last, still return K best")
	K := 10
	for pq.Len() > K {
		pq.Pop()
	}
	res := make([]*Item, K)
	for i := K - 1; i >= 0; i-- {
		res[i] = pq.Pop()
	}
	for i := 0; i < len(res); i++ {
		t.Logf("%+v", res[i])
	}
}

func TestBasicOne(t *testing.T) {
	pq := &DistQueueClosestFirst{}
	pq.Push(&node.Node{}, float32(20))
	pq.Push(&node.Node{}, float32(10))
	pq.Push(&node.Node{}, float32(15))
	for i := 1; i <= pq.Len(); i++ {
		t.Logf("internal %d=%f", i, pq.items[i].D)
	}
	correct := []float32{10, 15, 20}
	for i := 0; i < pq.Len(); i++ {
		x := pq.Pop()
		t.Logf("popped %d %f", i, x.D)
		assert.Equal(t, correct[i], x.D)
		for j, in := range pq.items {
			if j != 0 {
				t.Logf("internal %d=%f", j, in.D)
			}
		}
	}
}

func TestBasicTwo(t *testing.T) {
	pq := &DistQueueClosestFirst{}
	pq.Push(&node.Node{}, float32(20))
	pq.Push(&node.Node{}, float32(10))
	pq.Push(&node.Node{}, float32(15))
	pq.Push(&node.Node{}, float32(5))
	for i := 1; i <= pq.Len(); i++ {
		t.Logf("internal %d=%f", i, pq.items[i].D)
	}
	correct := []float32{5, 10, 15, 20}
	for i := 0; i < pq.Len(); i++ {
		x := pq.Pop()
		t.Logf("popped %d %f", i, x.D)
		assert.Equal(t, correct[i], x.D)
		for j, in := range pq.items {
			if j != 0 {
				t.Logf("internal %d=%f", j, in.D)
			}
		}
	}
}

func TestBasicThree(t *testing.T) {
	pq := &DistQueueClosestFirst{}
	pq.Push(&node.Node{}, float32(20))
	pq.Push(&node.Node{}, float32(10))
	pq.Push(&node.Node{}, float32(15))
	pq.Push(&node.Node{}, float32(5))
	pq.Push(&node.Node{}, float32(45))
	pq.Push(&node.Node{}, float32(75))
	pq.Push(&node.Node{}, float32(85))
	pq.Push(&node.Node{}, float32(95))
	pq.Push(&node.Node{}, float32(30))
	for i := 1; i <= pq.Len(); i++ {
		t.Logf("internal %d=%f", i, pq.items[i].D)
	}
	correct := []float32{5, 10, 15, 20, 30, 45, 75, 85, 95}
	for i := 0; i < pq.Len(); i++ {
		x := pq.Pop()
		t.Logf("popped %d %f", i, x.D)
		assert.Equal(t, correct[i], x.D)
		for j, in := range pq.items {
			if j != 0 {
				t.Logf("internal %d=%f", j, in.D)
			}
		}
	}
}
