package service

import "util"

type BudgetSort [](map[string]string)

func (buget BudgetSort) Len() int{
	return len(buget)
}

func (buget BudgetSort) Swap(i,j int){
	buget[i],buget[j] = buget[j],buget[i]
}

func (buget BudgetSort) Less(i,j int) bool{
	n1 := GetSortX(buget[i],util.StringToFloat(buget[i]["dailyMaxFinal"]),util.StringToFloat(buget[i]["priceMaxFinal"]))
	n2 := GetSortX(buget[j],util.StringToFloat(buget[j]["dailyMaxFinal"]),util.StringToFloat(buget[j]["priceMaxFinal"]))
	return n1 > n2
}