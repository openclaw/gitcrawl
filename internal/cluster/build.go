package cluster

import "sort"

type Node struct {
	ThreadID int64
	Number   int
	Title    string
}

type Edge struct {
	LeftThreadID  int64
	RightThreadID int64
	Score         float64
}

type Cluster struct {
	RepresentativeThreadID int64   `json:"representative_thread_id"`
	Members                []int64 `json:"members"`
}

func Build(nodes []Node, edges []Edge) []Cluster {
	uf := newUnionFind()
	for _, node := range nodes {
		uf.add(node.ThreadID)
	}
	for _, edge := range edges {
		uf.union(edge.LeftThreadID, edge.RightThreadID)
	}

	byRoot := map[int64][]int64{}
	for _, node := range nodes {
		root := uf.find(node.ThreadID)
		byRoot[root] = append(byRoot[root], node.ThreadID)
	}
	return format(nodes, edges, byRoot)
}

func format(nodes []Node, edges []Edge, byRoot map[int64][]int64) []Cluster {
	edgeCounts := map[int64]int{}
	for _, edge := range edges {
		edgeCounts[edge.LeftThreadID]++
		edgeCounts[edge.RightThreadID]++
	}
	nodesByID := map[int64]Node{}
	for _, node := range nodes {
		nodesByID[node.ThreadID] = node
	}

	out := make([]Cluster, 0, len(byRoot))
	for _, members := range byRoot {
		sort.Slice(members, func(i, j int) bool { return members[i] < members[j] })
		representative := members[0]
		for _, member := range members[1:] {
			if betterRepresentative(member, representative, edgeCounts, nodesByID) {
				representative = member
			}
		}
		out = append(out, Cluster{RepresentativeThreadID: representative, Members: members})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if len(out[i].Members) == len(out[j].Members) {
			return out[i].RepresentativeThreadID < out[j].RepresentativeThreadID
		}
		return len(out[i].Members) > len(out[j].Members)
	})
	return out
}

func betterRepresentative(candidate, current int64, edgeCounts map[int64]int, nodesByID map[int64]Node) bool {
	if edgeCounts[candidate] != edgeCounts[current] {
		return edgeCounts[candidate] > edgeCounts[current]
	}
	candidateNode := nodesByID[candidate]
	currentNode := nodesByID[current]
	if candidateNode.Number != currentNode.Number {
		return candidateNode.Number < currentNode.Number
	}
	return candidate < current
}

type unionFind struct {
	parent map[int64]int64
	size   map[int64]int
}

func newUnionFind() *unionFind {
	return &unionFind{parent: map[int64]int64{}, size: map[int64]int{}}
}

func (u *unionFind) add(value int64) {
	if _, ok := u.parent[value]; !ok {
		u.parent[value] = value
		u.size[value] = 1
	}
}

func (u *unionFind) find(value int64) int64 {
	parent, ok := u.parent[value]
	if !ok {
		u.add(value)
		return value
	}
	current := value
	for parent != current {
		grandparent := u.parent[parent]
		u.parent[current] = grandparent
		current = parent
		parent = grandparent
	}
	return parent
}

func (u *unionFind) union(left, right int64) {
	leftRoot := u.find(left)
	rightRoot := u.find(right)
	if leftRoot == rightRoot {
		return
	}
	if u.size[leftRoot] < u.size[rightRoot] {
		leftRoot, rightRoot = rightRoot, leftRoot
	}
	u.parent[rightRoot] = leftRoot
	u.size[leftRoot] += u.size[rightRoot]
}
