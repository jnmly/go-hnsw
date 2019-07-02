package hnsw

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

type Result struct {
	ID       uint32
	Distance float32
}

func Search(h *Hnsw, q []float32) []Result {
	const (
		cfgEfSearch = 2000
		cfgK        = 5
	)

	ret := make([]Result, cfgK)
	//result := h.Search(q, cfgEfSearch, cfgK)
	result := h.SearchBrute(q, cfgK)
	for i := 1; !result.Empty(); i++ {
		x := result.Pop()
		ret[cfgK-i] = Result{ID: x.ID, Distance: x.D}
	}
	return ret
}

func TestSimple(t *testing.T) {
	const (
		cfgM              = 32
		cfgEfConstruction = 2000
		dimsize           = 128

		testrecords = 1000
	)

	var zero Point = make([]float32, dimsize)
	h := New(cfgM, cfgEfConstruction, zero)
	h.Grow(testrecords)

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

	for i := 1; i < testrecords; i++ {
		h.Add(vecs[i-1], uint32(i))
	}

	q := vecs[testrecords-1]

	res := Search(h, q)
	for _, dp := range res {
		t.Logf("dist=%f %d", dp.Distance, dp.ID)
	}
}
