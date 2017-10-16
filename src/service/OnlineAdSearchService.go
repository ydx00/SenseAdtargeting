package service

import (
	"util"
	"fmt"
)

func Search(appId string,userId string,broadcasterId string,adMode int,requestId string,version int){
    client := util.NewRedisClient()

	//startTime := time.Now()
	//
	//conditions := set.NewHashSet()
	//fansConditions := []string{}
	//cptConditions := set.NewHashSet()
	//cptCondition := fmt.Sprintf(util.AD_DM_SENSEAR_AD_STATIC_CPT,appId,":",adMode)

	fansInfo := client.HGetAll(util.REDIS_BDP_REALTIME,util.REDIS_DB_BDP_REALTIME,util.AD_BDP_SENSEAR_USER_INFO + appId + ":" + userId)
    for k,v := range fansInfo{
    	fmt.Fprintln(k)
    	fmt.Fprintln(v)
	}

}
