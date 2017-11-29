package DNF


type InvertedIndex struct {
	kernelPtr AssignmentStorage
	kvPtr CellStorage
}

func NewInvertedIndex() *InvertedIndex{
	return &InvertedIndex{
		kernelPtr:*NewAssignmentStorage(),
		kvPtr:*NewCellStorage(),
	}
}

func (invertedIndex *InvertedIndex) GetSize() int{
	return invertedIndex.kernelPtr.size_
}

func (invertedIndex *InvertedIndex) GetGroupSize() int{
	return invertedIndex.kernelPtr.GetGroupSize()
}




func (invertedIndex *InvertedIndex) InnerMatch(matched []Assignment, QueryAssi Assignment, kv CellStorage){
	invertedIndex.kernelPtr.Match(matched,QueryAssi,kv)
}

func (inveredIndex *InvertedIndex) MidMatch(matched map[Conjunction]int, QueryRela int, InnerMatched []Assignment){
	ConjCounter := make(map[Conjunction]int)
	for _,assi := range InnerMatched{
		conj := assi.conjPtr_
		if _,ok := ConjCounter[conj]; ok{
			ConjCounter[conj] ++
		}else {
			ConjCounter[conj] = 1
		}
	}
	for key,value := range ConjCounter{
		if value == QueryRela {
			if _,ok := matched[key]; ok {
                matched[key] ++
			}else {
				matched[key] = 1
			}
		}
	}
}

func (inveredIndex *InvertedIndex) OuterMatch(matched []Doc,MidMatched map[Conjunction]int){
	for key,value := range MidMatched{
		if key.size_ == 0 || key.size_ == value {
			matched = append(matched, key.docPtr)
		}
	}
}


