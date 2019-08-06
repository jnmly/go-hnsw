package framework

type LinkMap struct {
	Nodes map[uint64]bool
}

type LinkList struct {
	Nodes []uint64
}

type Node struct {
	P              []float32
	Level          uint64
	Friends        map[uint64]*LinkList
	ReverseFriends map[uint64]*LinkMap
	Id             uint64
}

type Hnsw struct {
	M              uint64
	M0             uint64
	EfConstruction uint64
	DelaunayType   uint64
	LevelMult      float64
	MaxLayer       uint64
	Sequence       uint64
	CountLevel     map[uint64]uint64
	Enterpoint     uint64
	Nodes          map[uint64]*Node
}
