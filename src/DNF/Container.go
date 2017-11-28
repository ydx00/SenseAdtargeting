package DNF

type Document struct {
	id string
	size int
}

type Conjunction struct {
	docIndex int
	size int
}

type Assignment struct {
	conjIndex int
	name string
	value string
	relation int
	conjSize int
}

type Index struct {
	docs []Document
	conjunctions []Conjunction
	assignments []Assignment
	assignmentIndexesMap map[int](map[string]([]int))
}
