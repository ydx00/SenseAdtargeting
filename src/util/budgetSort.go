package util

import "service"

type BudgetSort [](map[string]string)

func (buget BudgetSort) Len() int{
	return len(buget)
}

func (buget BudgetSort) Swap(i,j int){
	buget[i],buget[j] = buget[j],buget[i]
}

func (buget BudgetSort) Less(i,j int) bool{
	n1 := service.GetSortX(buget[i],StringToFloat(buget[i]["dailyMaxFinal"]),StringToFloat(buget[i]["priceMaxFinal"]))
	n2 := service.GetSortX(buget[j],StringToFloat(buget[j]["dailyMaxFinal"]),StringToFloat(buget[j]["priceMaxFinal"]))
	return n1 > n2
}