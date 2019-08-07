package hnsw

import (
	"fmt"
)

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
