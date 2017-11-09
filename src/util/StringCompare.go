package util

import (
	"strings"
	"strconv"
)

type AdArraySort []string

func (arrSort AdArraySort) Len() int{
	return len(arrSort)
}

func (arrSort AdArraySort) Swap(i, j int){
	arrSort[i], arrSort[j] = arrSort[j], arrSort[i]
}

func (arrSort AdArraySort) Less(i, j int) bool{
	sortA,errA := strconv.ParseFloat(strings.Split(arrSort[i], "_")[1], 32)
	sortB,errB := strconv.ParseFloat(strings.Split(arrSort[j], "_")[1], 32)
	if errA != nil && errB != nil{
		return true
	}else {
		return sortA > sortB
	}
}
