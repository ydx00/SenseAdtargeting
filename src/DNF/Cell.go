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

func (cell *Cell) GetName() string{
	return cell.name_
}

func (cell *Cell) GetValue() string{
	return cell.value_
}


/**
   The structure of CellStorage
 */
type CellStorage struct {
	storage map[string](map[string](*Cell))
}

func NewCellStorage() *CellStorage{
	return &CellStorage{
		storage:nil,
	}
}

func (cellStorage *CellStorage) Exist(targetPtr *Cell,cellPtr *Cell) bool{
	if value,ok := cellStorage.storage[cellPtr.GetName()]; ok{
		if _,exist := value[cellPtr.GetValue()];exist {
			targetPtr = value[cellPtr.GetValue()]
			return true
		}
	}
	return false
}

func (cellStorage *CellStorage) Get(targetPtr *Cell,cellPtr *Cell){
	if value,ok := cellStorage.storage[cellPtr.GetName()]; ok{
		if _,exist := value[cellPtr.GetValue()]; exist {
			targetPtr = value[cellPtr.GetValue()]
		}else {
			targetPtr = cellPtr
			cellStorage.storage[cellPtr.GetName()][cellPtr.GetValue()] = targetPtr
		}
	}else {
		targetPtr = cellPtr
		TmpV := make(map[string]Cell)
		TmpV[cellPtr.GetValue()] = *targetPtr
		cellStorage.storage[cellPtr.GetName()] = TmpV
	}
}

func (cellStorage *CellStorage) Size() int{
	return len(cellStorage.storage)
}

