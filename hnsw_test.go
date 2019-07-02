package hnsw

import (
	"io/ioutil"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/assert"
)

type Result struct {
	ID       uint32
	Distance float32
}

const (
	testrecords = 1000
	dimsize     = 128
)

func Search(h *Hnsw, q []float32) []Result {
	const (
		cfgEfSearch = 2000
		cfgK        = 1000
	)

	ret := make([]Result, cfgK)
	result := h.Search(q, cfgEfSearch, cfgK)
	//result := h.SearchBrute(q, cfgK)
	for i := 1; !result.Empty(); i++ {
		x := result.Pop()
		ret[cfgK-i] = Result{ID: x.ID, Distance: x.D}
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

	var zero Point = make([]float32, dimsize)
	h := New(cfgM, cfgEfConstruction, zero)
	h.Grow(testrecords)

	return h
}

func TestSimple(t *testing.T) {
	h := newHnsw()
	q, vecs := getTestdata(t)

	count := 1
	for _, v := range vecs {
		h.Add(v, uint32(count))
		count++
	}

	res := Search(h, q)

	cupaloy.SnapshotT(t, h.Print(), h.Stats(), res)
}

func TestSkip(t *testing.T) {
	h := newHnsw()
	q, vecs := getTestdata(t)

	count := 1
	for i, v := range vecs {
		if i != 500 {
			h.Add(v, uint32(count))
			count++
		}
	}

	res := Search(h, q)

	cupaloy.SnapshotT(t, h.Print(), h.Stats(), res)
}
