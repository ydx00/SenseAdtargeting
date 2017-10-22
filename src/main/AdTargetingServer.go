package main

import (
	"util"
	"log"
	"fmt"
)

/**
  rpc服务框架
*/


const (
	NetworkAddr = "127.0.0.1:19090"
)


func main() {
	client := util.GetBuntDBInstance()
	if client == nil {
		log.Fatal("获取数据库失败")
	}
	value := make(map[string]string)
    value["1"] = "2"
    value["2"] = "3"
	client.WriteMap("test",value,util.CPM_ADINFO_DB)
    data,err := client.ReadMap("test",util.CPM_ADINFO_DB)
	if err != nil {
		log.Fatal("读取数据失败")
	}
	for k,v := range data{
		fmt.Println(string(k) + v)
	}
}
