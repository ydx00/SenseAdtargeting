package DNF


/**
   The structure of Document
 */
type Doc struct {
	id_ string
	size_ int
	score_ float64
	max_show_times_ int
}

func NewDoc() *Doc{
	return &Doc{
		id_:"",
		size_:-1,
		score_:0.0,
		max_show_times_:0,
	}
}

func NewDocWithParam(id string,size int,score float64,max_show_times int) *Doc{
	return &Doc{
		id_:id,
		size_:size,
		score_:score,
		max_show_times_:max_show_times,
	}
}

func (doc *Doc) GetId() string{
	return doc.id_
}

func (doc *Doc) GetScore() float64{
	return doc.score_
}

func (doc *Doc) GetMaxShowTimes() int{
	return doc.max_show_times_
}

func (doc *Doc) GetSize() int{
	return doc.size_
}

func (doc *Doc) SetSize(size int){
	doc.size_ = size
}


/**
   The structure of Conjunction
 */
type Conjunction struct {
	docPtr *Doc
	size_ int
}

func NewConjunction(docPtr *Doc,size int) *Conjunction{
	return &Conjunction{
		docPtr:docPtr,
		size_:size,
	}
}

func (conjunction *Conjunction) GetDoc() *Doc{
	return conjunction.docPtr
}

func (conjunction *Conjunction) Getsize() int{
	return conjunction.size_
}

func (conjuntion *Conjunction) GetId() string{
	return conjuntion.docPtr.GetId()
}

/**
   The structure of Assignment
 */
type Assignment struct{
	conjPtr_ *Conjunction
    cellPtr_ *Cell
    relation_ int
}

func NewAssignment(conjPtr *Conjunction, cellPtr *Cell, relation int) *Assignment{
	return &Assignment{
		conjPtr_:conjPtr,
		cellPtr_:cellPtr,
		relation_:relation,
	}
}

func (assignment *Assignment) GetRelation() int{
	return assignment.relation_
}

func (assignment *Assignment) GetConjuction() *Conjunction{
    return assignment.conjPtr_
}

func (assignment *Assignment) GetName() string{
	return assignment.cellPtr_.GetName()
}

func (assignment *Assignment) GetValue() string{
	return assignment.cellPtr_.GetValue()
}

func (assignment *Assignment) Getsize() int{
	return assignment.conjPtr_.Getsize()
}

func (assignment *Assignment) GetId() string{
	return assignment.conjPtr_.GetId()
}

/**
   The structure of AssignmentStore
 */
type AssignmentStorage struct {
	storage map[int](map[*Cell](map[int]([](*Assignment))))
	size_ int
}

func NewAssignmentStorage() *AssignmentStorage{
	return &AssignmentStorage{
		size_:0,
	}
}

func (assignmentStorage *AssignmentStorage) GetSize() int{
	return assignmentStorage.size_
}

func (assignmentStorage *AssignmentStorage) GetGroupSize() int{
	return len(assignmentStorage.storage)
}

//func (assignmentStorage *AssignmentStorage) Match(matched [](*Assignment), QueryAssi *Assignment,kv *CellStorage){
//	for s := QueryAssi.Getsize(); s >= 0; s ++{
//		if _,ok := assignmentStorage.storage[s];ok{
//			var QueryCellPtr *Cell
//			var QueryRela int
//			if s == 0 {
//				kv.Get(QueryCellPtr,)
//			}
//		}
//	}
//}
