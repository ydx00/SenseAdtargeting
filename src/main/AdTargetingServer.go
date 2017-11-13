package main

import (
  "util"
)

/**
  rpc服务框架
*/


func main() {
    //service.OfflineAdStaticInfoProcess()
    redisclient := util.NewRedisClient()
	m2 := make(map[string]([]string))
	m2["test"] = []string{"aa","bb"}
	m2["test2"] = []string{"cc","dd"}
	//m1 := []string{"1","2"}
	//redisclient.Lset(util.REDIS_SENSEAR,10,"test",m1)
	redisclient.LsetByPipeline(util.REDIS_SENSEAR,10,m2,100)

}
