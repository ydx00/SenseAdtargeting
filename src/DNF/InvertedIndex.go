package DNF

import (
	"sync"
	"util"
)

type InvertedIndex struct {
	mutex sync.Mutex
	index *Index
	redisClient *util.RedisClient
}

func NewInvertedIndex() *InvertedIndex {
	redisClient := util.NewRedisClient()
    index := &Index{
    	docs:nil,
		conjunctions:nil,
		assignments:nil,
		assignmentIndexesMap:nil,
	}
	return &InvertedIndex{
		index:index,
		redisClient:redisClient,
	}
}

func (invertedIndex *InvertedIndex) MatchingAssignment(qry *Assignment,matchingAssignmentIndexes []int){
	assignmentKey := qry.name + "_" + qry.value
	for i := qry.conjSize; i >= 0; i--{
		 if value,ok := invertedIndex.index.assignmentIndexesMap[i];ok{
			 if _,exist := value[assignmentKey];exist{
				 for _,idx := range value[assignmentKey]{
					 if invertedIndex.index.assignments[idx].relation >= qry.relation {
						 matchingAssignmentIndexes = append(matchingAssignmentIndexes,idx)
					 }
				 }
			 }
		 }
	}
}

func (invertedIndex *InvertedIndex) CountingEachConjunction(matchingAssignmentIndexes []int,ConjunctionByCount map[int]int){
	for _,idx := range matchingAssignmentIndexes{
		conjIndex := invertedIndex.index.assignments[idx].conjIndex
        if _,ok := ConjunctionByCount[conjIndex]; ok{
			ConjunctionByCount[conjIndex] ++
		}else {
			ConjunctionByCount[conjIndex] = 1
		}
	}
}

func (invertedIndex *InvertedIndex) FilteringConjByEachQueryAssi(ConjunctionByCount map[int]int, sizeofAttibute int,matchingConjunction map[int]int){
    for key,value := range ConjunctionByCount{
    	if value == sizeofAttibute{
			if _,ok := matchingConjunction[key]; ok {
				matchingConjunction[key] ++
			}else {
				matchingConjunction[key] = 1
			}
		}
	}
}

func (invertedIndex *InvertedIndex) MatchingDoc(matchingConjunction map[int]int, matchingDocsIDs []string){
    for key,value := range matchingConjunction{
		if value == invertedIndex.index.conjunctions[key].size{
            docIndex := invertedIndex.index.conjunctions[key].docIndex
            matchingDocsIDs = append(matchingDocsIDs,invertedIndex.index.docs[docIndex].id)
		}
	}
}

//func (invertedIndex *InvertedIndex) LoadFromFile(){
//	if len(invertedIndex.index.assignments) > 0 {
//		log.Println("The assignmentIndexesMap is already existed")
//		return
//	}
//	for index,assignment := range invertedIndex.index.assignments{
//		assignmentKey := assignment.name + "_" + assignment.value
//		conjSize := assignment.conjSize
//
//	}
//}







