package util

import (
	"strconv"
	"strings"
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

func StringToListStr(str string) []string{
	if str != "" {
		return strings.Split(str,",")
	} else{
		return make([]string,0)
	}
}

func StringToInt(str string) int{
	value,err := strconv.Atoi(str)
	if err != nil {
		panic(err)
	}
	return value
}

