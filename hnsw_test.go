package hnsw

import (
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"testing"
	"time"

	"github.com/jnmly/go-hnsw/node"
	"github.com/stretchr/testify/assert"
)

type Result struct {
	Node     node.NodeRef
	Distance float32
}

const (
	testrecords = 1000
	dimsize     = 128
)

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

	var zero node.Point = make([]float32, dimsize)
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
	err := ioutil.WriteFile(fmt.Sprintf("/tmp/state.%d", i), []byte(h.Print()), os.ModePerm)
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
	n := h.nodes[500]
	h.Remove(500)
	dumpState(h, 9999)

	Search(h, q)

	for _, nn := range h.nodes {
		for level := h.maxLayer; level < math.MaxUint64; level-- {
			for _, x := range nn.GetFriends(level) {
				if h.nodes[x] == n {
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

		if i > 4 && h.enterpoint == n && h.nodes[n].Level == h.maxLayer {
			h.Remove(n)
			break
		}
	}

	Search(h, q)

	found := false
	for i, _ := range h.nodes {
		if i == h.enterpoint {
			found = true
			break
		}
	}

	if !found {
		t.Fail()
	}
}
