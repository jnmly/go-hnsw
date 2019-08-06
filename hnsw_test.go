package hnsw

import (
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jnmly/go-hnsw/framework"
	"github.com/stretchr/testify/assert"
)

type Result struct {
	Node     uint64
	Distance float32
}

const (
	testrecords = 1000
	dimsize     = 128
)

func Print(h *Hnsw) string {
	buf := strings.Builder{}

	buf.WriteString(fmt.Sprintf("enterpoint = %d %p\n", h.Enterpoint, h.Nodes[h.Enterpoint]))

	for i, n := range h.Nodes {
		buf.WriteString(fmt.Sprintf("node %d, level %d, addr %p\n", i, n.Level, n))
		for lvl, arr := range n.Friends {
			for friendindex, f := range arr.Nodes {
				buf.WriteString(fmt.Sprintf("     level %d friend %d = %d\n", lvl, friendindex, f))
			}
		}
		buf.WriteString("\n\n\n")
	}

	return buf.String()
}

type u64array []uint64

func FullState(h *Hnsw) string {
	buf := strings.Builder{}

	buf.WriteString(fmt.Sprintf("M = %v\n", h.M))
	buf.WriteString(fmt.Sprintf("M0 = %v\n", h.M0))
	buf.WriteString(fmt.Sprintf("EfConstruction = %v\n", h.EfConstruction))
	buf.WriteString(fmt.Sprintf("DelaunayType = %v\n", h.DelaunayType))
	buf.WriteString(fmt.Sprintf("LevelMult = %v\n", h.LevelMult))
	buf.WriteString(fmt.Sprintf("MaxLayer = %v\n", h.MaxLayer))
	buf.WriteString(fmt.Sprintf("Sequence = %v\n", h.Sequence))
	buf.WriteString(fmt.Sprintf("Enterpoint = %v\n", h.Enterpoint))
	//Nodes

	keys := make(u64array, 0, len(h.CountLevel))
	for k := range h.CountLevel {
		keys = append(keys, k)
	}
	sort.Sort(keys)
	for _, k := range keys {
		buf.WriteString(fmt.Sprintf("CountLevel[%d] = %d\n", k, h.CountLevel[k]))
	}

	keys = make(u64array, 0, len(h.Nodes))
	for k := range h.Nodes {
		keys = append(keys, k)
	}
	sort.Sort(keys)
	for _, k := range keys {
		buf.WriteString(fmt.Sprintf("  node[%d].Id = %d\n", k, h.Nodes[k].Id))
		buf.WriteString(fmt.Sprintf("  node[%d].Level = %d\n", k, h.Nodes[k].Level))
		buf.WriteString(fmt.Sprintf("  node[%d].P = %v\n", k, h.Nodes[k].P))
		buf.WriteString("")

		friendkeys := make(u64array, 0, len(h.Nodes[k].Friends))
		for k := range h.Nodes[k].Friends {
			friendkeys = append(friendkeys, k)
		}
		sort.Sort(friendkeys)

		for _, f := range friendkeys {
			for _, fn := range h.Nodes[k].Friends[f].Nodes {
				buf.WriteString(fmt.Sprintf("    node[%d].friend[%d] = %v\n", k, f, fn))
			}
		}

		reversefriendkeys := make(u64array, 0, len(h.Nodes[k].ReverseFriends))
		for k := range h.Nodes[k].ReverseFriends {
			reversefriendkeys = append(reversefriendkeys, k)
		}
		sort.Sort(reversefriendkeys)
		for _, f := range reversefriendkeys {
			innerkeys := make(u64array, 0, len(h.Nodes[k].ReverseFriends[f].Nodes))
			for i := range h.Nodes[k].ReverseFriends[f].Nodes {
				innerkeys = append(innerkeys, i)
			}
			sort.Sort(innerkeys)

			for _, rl := range innerkeys {
				buf.WriteString(fmt.Sprintf("    node[%d].revfriend[%d] = %v\n", k, f, rl))
			}
		}
	}

	return buf.String()
}

func (u u64array) Len() int {
	return len(u)
}

func (u u64array) Less(i int, j int) bool {
	return u[i] < u[j]
}

func (u u64array) Swap(i int, j int) {
	u[i], u[j] = u[j], u[i]
}

func Search(h *Hnsw, q []float32) []Result {
	//fmt.Printf("entered test Search\n")
	//defer fmt.Printf("left test Search\n")

	const (
		cfgEfSearch = 2000
		cfgK        = 50
	)

	ret := make([]Result, cfgK)
	result := h.Search(q, cfgEfSearch, cfgK)
	//result := h.SearchBrute(q, cfgK)
	for i := 1; !result.Empty(); i++ {
		x := result.Pop()
		ret[cfgK-i] = Result{Node: x.Node, Distance: x.D}
	}
	return ret
}

func getTestdata(t *testing.T) ([]float32, [][]float32) {
	data, err := ioutil.ReadFile("testdata/data.txt")
	assert.NoError(t, err)

	vecs := make([][]float32, testrecords)
	for i := 0; i < testrecords; i++ {
		vecs[i] = make([]float32, dimsize)
		for j := 0; j < dimsize; j++ {
			f := float32(data[i*dimsize+j])
			vecs[i][j] = f
		}
	}

	q := vecs[0]
	vecs = vecs[1:]

	return q, vecs
}

func newHnsw() *Hnsw {
	const (
		cfgM              = 32
		cfgEfConstruction = 2000
	)

	var zero framework.Point = make([]float32, dimsize)
	h := New(cfgM, cfgEfConstruction, zero)

	return h
}

func TestSimple(t *testing.T) {
	h := newHnsw()
	q, vecs := getTestdata(t)

	for _, v := range vecs {
		h.Add(v)
	}

	t0 := time.Now()
	Search(h, q)
	t.Logf("searchtime = %v", time.Since(t0))
}

func dumpState(h *Hnsw, i int) {
	err := ioutil.WriteFile(fmt.Sprintf("/tmp/state.%d", i), []byte(FullState(h)), os.ModePerm)
	if err != nil {
		panic(err)
	}
}

func TestRemove(t *testing.T) {
	h := newHnsw()
	q, vecs := getTestdata(t)

	for i, v := range vecs {
		h.Add(v)
		if i >= 497 && i <= 503 {
			dumpState(h, i)
		}
	}

	dumpState(h, 9998)
	n := h.Nodes[500]
	h.Remove(500)
	dumpState(h, 9999)

	Search(h, q)

	for _, nn := range h.Nodes {
		for level := h.MaxLayer; level < math.MaxUint64; level-- {
			for _, x := range nn.GetNodeFriends(level) {
				if h.Nodes[x] == n {
					t.FailNow()
				}
			}
		}
	}
}

func TestEnterPointRemove(t *testing.T) {
	h := newHnsw()
	q, vecs := getTestdata(t)

	for i, v := range vecs {
		n := h.Add(v)

		if i > 4 && h.Enterpoint == n && h.Nodes[n].Level == h.MaxLayer {
			h.Remove(n)
			break
		}
	}

	Search(h, q)

	found := false
	for i, _ := range h.Nodes {
		if i == h.Enterpoint {
			found = true
			break
		}
	}

	if !found {
		t.Fail()
	}
}

func TestLoadSave(t *testing.T) {
	h := newHnsw()
	_, vecs := getTestdata(t)
	for _, v := range vecs {
		h.Add(v)
	}

	data, err := h.Marshal()
	assert.NoError(t, err)
	t.Logf("data is %d long", len(data))
	n := len(h.Nodes)

	g := &Hnsw{}
	err = g.Unmarshal(data)
	assert.NoError(t, err)
	assert.Equal(t, n, len(g.Nodes))
	t.Logf("there are %d nodes", len(g.Nodes))

	assert.Equal(t, FullState(h), FullState(g))
}

func TestLocking(t *testing.T) {
	h := newHnsw()
	_, vecs := getTestdata(t)
	wg := &sync.WaitGroup{}

	dups := make(chan uint64, len(vecs))

	for _, v := range vecs {
		wg.Add(4)
		go func() {
			nodeId := h.Add(v)
			dups <- nodeId
			wg.Done()
		}()
		go func() {
			h.Search(v, 2000, 5)
			wg.Done()
		}()
		go func() {
			nodeId := <-dups
			h.Remove(nodeId)
			wg.Done()
		}()
		go func() {
			h.Stats()
			wg.Done()
		}()
	}

	wg.Wait()
}
