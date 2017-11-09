package main

import (
	"util"
)

/**
  rpc服务框架
*/


const (
	NetworkAddr = "127.0.0.1:19090"
)


func main() {
	confighelp := util.NewConfigHelper()
	print(confighelp.ConfigMap["SENSEAR_REDIS_SERVER_HOST"])
}
