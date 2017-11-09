package service

import (
	"log"
	"util"
	"time"
)


func offlineAdStaticInfoProcess(){
	appIdList := GetAllApps()
	log.Println("adList.size:"+string(len(appIdList)))

	task_fre := util.StringToInt(util.NewConfigHelper().ConfigMap["AD_STATIC_INFO_TASK_FRE"])

	for _,appId := range appIdList{
		adList := GetMediaAllAds(appId)

		if len(adList) == 0 {
			continue
		}
		cpmResult := make(map[string]([]string))
		cptResult := make(map[string]([]string))

		targetCpmAdvs := make(map[string]([](map[string]string)))

		//defaultAdIdList := make([]string,0)
		//处理CPT广告


	}

}

//func fpCPTPredicate(adver map[string]string,plusDays int) bool{
//	flag := false
//	costType := adver["cost_type"]
//	adMode := adver["ad_mode"]
//
//	startDate := util.GetIntValueFromMap(adver,"start_date",0)
//	endDate := util.GetIntValueFromMap(adver,"end_date",0)
//
//	atDate := false
//	if endDate == 0 {
//		atDate = true
//	}else{
//		todayMills := time.Now().AddDate(0,0,plusDays)
//	}
//
//}
