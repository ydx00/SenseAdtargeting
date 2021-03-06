package util

import (
	"strconv"
	"strings"
	"time"
	"fmt"
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

func GetInt64ValueFromMap(dict map[string]string, key string, default_value int64) int64{
	if value,ok := dict[key]; ok {
		return StringtoInt64(value)
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

func StringToDuration(str string) time.Duration{
	duration := time.Duration(StringtoInt64(str))
	return duration
}

func StringToInt(str string) int{
	value,err := strconv.Atoi(str)
	if err != nil {
		panic(err)
	}
	return value
}

func StringtoInt64(str string) int64{
	value, err := strconv.ParseInt(str, 10, 64)
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

func FloatToString(value float64) string{
	return fmt.Sprintf("%f",value)
}

func IntToString(value int) string{
	return fmt.Sprintf("%d",value)
}

func Int64ToSting(value int64) string{
	return fmt.Sprintf("%d",value)
}

func BoolToString(value bool) string{
	return strconv.FormatBool(value)
}


func InterfaceToString(value interface{}) string{
	switch value.(type) {
	case bool:
        return BoolToString(value.(bool))
	case float64:
		return FloatToString(value.(float64))
	case int:
		return IntToString(value.(int))
	case int64:
		return Int64ToSting(value.(int64))
	case string:
		return value.(string)
	default:
		return ""
	}
}
