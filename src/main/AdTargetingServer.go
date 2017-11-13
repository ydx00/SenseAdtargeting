package main

import (
	"service"
)

/**
  rpc服务框架
*/


const (
	NetworkAddr = "127.0.0.1:19090"
)


func main() {
  service.OfflineAdRealtimeInfoProcess()
  service.OfflineAdStaticInfoProcess()
}
