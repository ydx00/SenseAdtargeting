package main

/**
  rpc服务框架
*/
import (
	"service"
	"util"
)

const (
	NetworkAddr = "127.0.0.1:19090"
)

func main() {
	requestId := "req" + "_" + "99"
    service.Search("app", "fans1", "broadcaster1",2,requestId,util.API_VERSION_OLD)
}

