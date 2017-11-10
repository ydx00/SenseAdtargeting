package util

import (
	"strconv"
	"strings"
	"flag"
)

func GetStringFromMap(dict map[string]string,key string,default_value string) string{
	if value,ok := dict[key]; ok {
		return value
	}else {
		return default_value
	}
}

func GetIntValueFromMap(dict map[string]string,key string,default_value int) int{
	if value,ok := dict[key]; ok {
		if result,err  := strconv.Atoi(value); err == nil{
			return result
		}
	}
	return default_value
}

func GetDoubleValueFromMap(dict map[string]string,key string,default_value float64) float64{
	if value,ok := dict[key]; ok {
		if result,err := strconv.ParseFloat(value,32); err == nil {
			return result
		}
	}
	return default_value
}

func StringToListStr(str string) []string{
	if str != "" {
		return strings.Split(str,",")
	} else{
		return make([]string,0)
	}
}

func StringToListInt(str string) []int{
	result := make([]int,0)
	if str != "" {
		for _,v := range strings.Split(str,","){
			result = append(result,StringToInt(v))
		}
	}
	return result
}

func StringToInt(str string) int{
	value,err := strconv.Atoi(str)
	if err != nil {
		panic(err)
	}
	return value
}

func StringToFloat(str string) float64{
	value,err := strconv.ParseFloat(str,64)
	if err != nil {
		panic(err)
	}
	return value
}

