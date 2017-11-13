package service

import (
	"strings"
	"util"
)

type AdArraySort []string

func (arrSort AdArraySort) Len() int{
	return len(arrSort)
}

func (arrSort AdArraySort) Swap(i, j int){
	arrSort[i], arrSort[j] = arrSort[j], arrSort[i]
}

func (arrSort AdArraySort) Less(i, j int) bool{
	sortA := util.StringToFloat(strings.Split(arrSort[i], "_")[1])
	sortB := util.StringToFloat(strings.Split(arrSort[j], "_")[1])
	return sortA > sortB
}
