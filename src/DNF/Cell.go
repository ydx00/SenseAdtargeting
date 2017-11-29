package DNF

/**
   The structure of Cell
 */
type Cell struct {
	name_ string
	value_ string
}

func NewCell() *Cell{
	return &Cell{
		name_:"",
		value_:"",
	}
}

func NewCellWithParam(name string, value string) *Cell{
	return &Cell{
		name_:name,
		value_:value,
	}
}



/**
   The structure of CellStorage
 */
type CellStorage struct {
	storage map[string](map[string]Cell)
}

func NewCellStorage() *CellStorage{
	return &CellStorage{
		storage:nil,
	}
}

func (cellStorage *CellStorage) Exist(targetPtr *Cell,cellPtr *Cell) bool{
	if value,ok := cellStorage.storage[cellPtr.name_]; ok{
		if _,exist := value[cellPtr.value_];exist {
			targetPtr = &value[cellPtr.value_]
			return true
		}
	}
	return false
}

func (cellStorage *CellStorage) Get(targetPtr *Cell,cellPtr *Cell){
	if value,ok := cellStorage.storage[cellPtr.name_]; ok{
		if _,exist := value[cellPtr.value_]; exist {
			targetPtr = &value[cellPtr.value_]
		}else {
			targetPtr = cellPtr
			cellStorage.storage[cellPtr.name_][cellPtr.value_] = *targetPtr
		}
	}else {
		targetPtr = cellPtr
		TmpV := make(map[string]Cell)
		TmpV[cellPtr.value_] = *targetPtr
		cellStorage.storage[cellPtr.name_] = TmpV
	}
}

func (cellStorage *CellStorage) Size() int{
	return len(cellStorage.storage)
}

