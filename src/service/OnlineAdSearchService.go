package service

import (
	"util"
	"fmt"
	"log"
	"set"
)
const(
	USE_REDIS = 1
)
func Search(appId string,userId string,broadcasterId string,adMode int,requestId string,version int){
    client := util.NewRedisClient()

	//startTime := time.Now()
	//
	//conditions := set.NewHashSet()
	//fansConditions := []string{}
	cptConditions := set.NewHashSet()
	cptCondition := fmt.Sprintf(util.AD_DM_SENSEAR_AD_STATIC_CPT,appId,":",adMode)

	fansInfo := client.HGetAll(util.REDIS_BDP_REALTIME,util.REDIS_DB_BDP_REALTIME,util.AD_BDP_SENSEAR_USER_INFO + appId + ":" + userId)
	if len(fansInfo) == 0 {
		log.Println("{\"requestId\":"+requestId+",\"info\":\"未能找到对应的粉丝信息的userId===="+userId+"\"}")
		cptCondition = cptCondition+":-1"
		cptConditions.Add(cptCondition)
	}else {
		if os, ok := fansInfo["os"]; ok {
			cptConditions.Add(cptCondition + ":" + os)
			cptConditions.Add(cptCondition + ":" + ":-1")
		} else {
			cptConditions.Add(cptCondition + ":" + ":-1")
		}
	}
	//fpCPTAds := set.NewSimpleSet()
	//if USE_REDIS == 1 {
	//	fpCPTAds = client.LGetAllTargetAdWithPipeLine(util.REDIS_DB_DM,cptConditions)
	//}else {
     //   for _,cptCon := range cptConditions.Elements(){
     //
	//	}
	//}




}
