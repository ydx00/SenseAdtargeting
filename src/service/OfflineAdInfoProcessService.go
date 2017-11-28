package service

import (
	"log"
	"util"
	"time"
	"github.com/buger/jsonparser"
	"strconv"
	"strings"
)

var buntDBclient = util.GetBuntDBInstance()
var USE_REDIS = util.StringToInt(util.NewConfigHelper().ConfigMap["USE_REDIS"])

func OfflineAdStaticInfoProcess(){
	appIdList := GetAllApps()
	log.Println("adList.size:"+util.IntToString(appIdList.Len()))

	ad_static_task_fre := util.StringToInt(util.NewConfigHelper().ConfigMap["AD_STATIC_INFO_TASK_FRE"])
	for _,appid := range appIdList.Elements(){
		appId := util.InterfaceToString(appid)
		adList := GetMediaAllAds(appId)
		//fmt.Println("adList:"+util.IntToString(len(adList)))
		if len(adList) == 0 {
			continue
		}

		cpmResult := make(map[string]([]string))
		cptResult := make(map[string]([]string))

		targetCpmAdvs := make(map[string]([](map[string]string)))

		//处理CPT广告
		cptAdList := make([](map[string]string),0)
		for _,adver := range adList{
			if fpCPTPredicate(adver,0) {
				cptAdList = append(cptAdList,adver)
			}
		}
		//fmt.Println("cptAdList:"+util.IntToString(len(cptAdList)))

		for _,adv := range cptAdList{
			aAudienceOss := util.StringToListInt(adv["audience_oss"])
			adMode := adv["ad_mode"]

			if len(aAudienceOss) > 0 {
				for _,os := range aAudienceOss{
					adCons := util.AD_DM_SENSEAR_AD_STATIC_CPT + appId + ":" +adMode + ":" + util.IntToString(os)
					if _,ok := cptResult[adCons]; ok {
                       cptResult[adCons] = append(cptResult[adCons],adv["advertisement_id"])
					}else {
						advIdList := make([]string,0)
						advIdList = append(advIdList,adv["advertisement_id"])
						cptResult[adCons] = advIdList
					}
				}
			}else {
				adCons := util.AD_DM_SENSEAR_AD_STATIC_CPT+appId+":"+adMode+":-1"
				if _,ok := cptResult[adCons]; ok {
					cptResult[adCons] = append(cptResult[adCons],adv["advertisement_id"])
				}else {
					advIdList := make([]string,0)
					advIdList = append(advIdList,adv["advertisement_id"])
					cptResult[adCons] = advIdList
				}
			}
		}

		if USE_REDIS == 1{
			redisClient.LsetByPipeline(util.REDIS_DM,util.REDIS_DB_DM,cptResult,ad_static_task_fre*60)
		}else{
			//fmt.Println(len(cptResult))
			for k,v := range cptResult{
				buntDBclient.WriteArr(k,v,util.CPT_ADINFO_DB)
			}
		}

		//处理CPM广告
		cpmAdList := make([](map[string]string),0)
		for _,adver := range adList{
			if fpCPMPredicate(adver,0) {
				cpmAdList = append(cpmAdList,adver)
			}
		}

        AddAdExInfo(cpmAdList)

		dailyMax := 0.0
		priceMax := 0.0

		for _,adv := range cpmAdList{
			aAudienceGenders := util.StringToListInt(adv["audience_genders"])
			aAudienceAgeGroups := util.StringToListInt(adv["audience_agegroups"])
			aAudienceOss := util.StringToListInt(adv["audience_oss"])

			aAudienceAreas := []int{-1}
			if _,ok := adv["areas"]; ok{
				aAudienceAreas = util.StringToListInt(adv["areas"])
			}
			aBroadcasterTags := []string{"-1"}
			if _,ok := adv["broadcaster_tags"]; ok{
				aBroadcasterTags = util.StringToListStr(adv["broadcaster_tags"])
			}

			adMode := adv["ad_mode"]

			adDaily := util.GetDoubleValueFromMap(adv,"daily_min",0.0)
			if adDaily > dailyMax{
				dailyMax = adDaily
			}

			price := util.GetDoubleValueFromMap(adv,"actual_price",0.0)
			if price > priceMax{
				priceMax = price
			}

			if "2" == adMode {
				for _,gender := range aAudienceGenders{
					for _,ageGroup := range aAudienceAgeGroups{
						for _,os := range aAudienceOss{
							for _,area := range aAudienceAreas{
								for _,bTag := range aBroadcasterTags{
									adCons := util.AD_DM_SENSEAR_AD_STATIC_CPM+appId+":"+adMode+":"+util.IntToString(gender)+"_"+util.IntToString(ageGroup)+"_"+util.IntToString(os)+"_"+util.IntToString(area)+"_"+bTag
									if _,ok := targetCpmAdvs[adCons]; ok {
										targetCpmAdvs[adCons] = append(targetCpmAdvs[adCons],adv)
									}else {
										advList := make([](map[string]string),0)
										advList = append(advList,adv)
										targetCpmAdvs[adCons] = advList
									}
								}
							}
						}
					}
				}
			}else if "5" == adMode{
				for _,gender := range aAudienceGenders {
					for _, ageGroup := range aAudienceAgeGroups {
						for _, os := range aAudienceOss {
							for _, area := range aAudienceAreas {
								adCons := util.AD_DM_SENSEAR_AD_STATIC_CPM+appId+":"+adMode+":"+strconv.Itoa(gender)+"_"+strconv.Itoa(ageGroup)+"_"+strconv.Itoa(os)+"_"+strconv.Itoa(area)
								if _,ok := targetCpmAdvs[adCons]; ok {
									targetCpmAdvs[adCons] = append(targetCpmAdvs[adCons],adv)
								}else {
									advList := make([](map[string]string),0)
									advList = append(advList,adv)
									targetCpmAdvs[adCons] = advList
								}
							}
						}
					}
				}
			}
		}
		//排序
		for targetKey,_ := range targetCpmAdvs{
			advList := targetCpmAdvs[targetKey]
            for _,adv := range advList{
            	GetSortX(adv,dailyMax,priceMax)
			}
			advList = Sort(advList)
			adIdList := make([]string,0)
			for _,adInfo := range advList{
				adIdList = append(adIdList,adInfo["advertisement_id"]+"_"+adInfo["sort"]+"_"+util.IntToString(getShowTimesLimit(adInfo)))
			}
			cpmResult[targetKey] = adIdList
		}
		if USE_REDIS == 1{
			redisClient.LsetByPipeline(util.REDIS_DM,util.REDIS_DB_DM,cpmResult,ad_static_task_fre*60)
		}else {
			for k,v := range cpmResult{
            	buntDBclient.WriteArr(k,v,util.CPM_ADINFO_DB)
			}
		}
		//处理广告模式CPT_NUM
		result := redisClient.HGetAll("SENSEAR",10,"SARA_KEY_APP_SIGNKEY:"+appId)

		if len(result) > 0 {
			ad_config := result["ad_configuration"]
			//if ad_config != ""{
			//	var returnData map[string]interface{}
			//	if err := json.Unmarshal([]byte(ad_config), &returnData); err == nil {
			//		//处理广告模式2
			//		if _,ok := returnData["2"]; ok {
			//			admode2data := returnData["2"]
			//			if value,ok := admode2data.(map[string]interface{}); ok{
			//				if len(value) > 0 {
			//					if _,ok := value["cpt_ad_num"]; ok{
			//						if admode2num,flag := (value["cpt_ad_num"]).(float64);flag && admode2num > 0{
			//							key := util.AD_MODE_CPT_AD_NUM + ":" + appId + ":2"
			//							buntDBclient.WriteInt(key,int(admode2num),util.ADMODE_NUM_DB)
			//						}
			//					}
			//				}
			//			}
			//		}
			//		//处理广告模式5
			//		if _,ok := returnData["5"]; ok {
			//			admode5data := returnData["5"]
			//			if value,ok := admode5data.(map[string]interface{}); ok{
			//				if len(value) > 0 {
			//					if _,ok := value["cpt_ad_num"]; ok{
			//						if admode5num,flag := (value["cpt_ad_num"]).(float64);flag && admode5num > 0{
			//							key := util.AD_MODE_CPT_AD_NUM + ":" + appId + ":5"
			//							buntDBclient.WriteInt(key,int(admode5num),util.ADMODE_NUM_DB)
			//						}
			//					}
			//				}
			//			}
			//		}
			//	}
			//}
			if ad_config != ""{
				//处理广告模式2
				if admode2num,err := jsonparser.GetInt([]byte(ad_config),"2","cpt_ad_num"); err == nil{
					log.Println("admode2num:",admode2num)
					if admode2num > 0{
						key := util.AD_MODE_CPT_AD_NUM + ":" + appId + ":2"
						buntDBclient.WriteInt(key,int(admode2num),util.ADMODE_NUM_DB)
					}
				}
				//处理广告模式5
				if admode5num,err := jsonparser.GetInt([]byte(ad_config),"5","cpt_ad_num"); err == nil{
					log.Println("admode5num:",admode5num)
					if admode5num > 0 {
						key := util.AD_MODE_CPT_AD_NUM + ":" + appId + ":5"
						buntDBclient.WriteInt(key,int(admode5num),util.ADMODE_NUM_DB)
					}
				}
			}
		}
	}
}

func OfflineAdRealtimeInfoProcess(){
	appIdList := GetAllApps()
	realtime_task_fre := util.StringToInt(util.NewConfigHelper().ConfigMap["AD_REALTIME_INFO_TASK_PRE"])
	log.Println("appIdList.size:"+util.IntToString(appIdList.Len()))
	for _,appid := range appIdList.Elements(){
		appId := util.InterfaceToString(appid)
		adList := GetMediaAllAds(appId)
		if len(adList) == 0 {
			continue
		}
		cpmResult := make(map[string](map[string]string))
		cptResult := make(map[string](map[string]string))

		cpmAdList := make([](map[string]string),0)
		cptAdList := make([](map[string]string),0)

		//处理CPT广告
		for _,adver := range adList{
			if fpCPTPredicate(adver,0) {
				cptAdList = append(cptAdList,adver)
			}
		}

		for _,adv := range cptAdList{
			checkMap := make(map[string]string)
			adId := adv["advertisement_id"]

			status := 0
			if util.GetIntValueFromMap(adv,"ad_status",1) == util.STATUS_AD_ENABLE {
				log.Println("{\"CPT有效性验证通过ID\":"+adId+"}")
				status = 1
			}

			checkMap["statusCheck"] = util.IntToString(status)

			cptResult[util.AD_DM_SENSEAR_AD_REALTIME_CPT+adId] = checkMap
		}

		for _,adver := range adList{
			if fpCPMPredicate(adver,0) {
				cpmAdList = append(cpmAdList,adver)
			}
		}

		AddAdExInfo(cpmAdList)

		for _,adv := range cpmAdList{
			checkMap := make(map[string]string)

			adId := adv["advertisement_id"]

			status := 0
			if util.GetIntValueFromMap(adv,"ad_status",1) == util.STATUS_AD_ENABLE {
				log.Println("{\"CPM有效性验证通过ID\":"+adId+"}")
				status = 1
			}

			balanceCheck := 0
			adCategory := adv["ad_category"]
			if util.StringToInt(adCategory) == util.ADVERTISER || util.StringToInt(adCategory) == util.MEDIA {
				dailyLimit := util.GetDoubleValueFromMap(adv,"daily_limit",0.0)
				available := util.GetDoubleValueFromMap(adv, "available",0.0)

				if available > 0 || dailyLimit == 0 {
					totalLimit := util.GetDoubleValueFromMap(adv,"total_limit",0.0)
					totalAvailable := util.GetDoubleValueFromMap(adv, "total_available", 0.0)
					if totalAvailable > 0  || totalLimit == 0 {
						if util.StringToInt(adCategory) == util.ADVERTISER {
							availableBalance := util.GetDoubleValueFromMap(adv, "available_balance", 0.0)
							if availableBalance > 0 {
								balanceCheck = 1
							}
						}else {
							balanceCheck = 1
						}
					}
				}
			}else {
				balanceCheck = 1
			}

			inTimeRangesCheck := 0
			adTimeranges := adv["timeranges"]

			nowHour := time.Now().Hour()

			if adTimeranges == "-1" {
				inTimeRangesCheck = 1
			}else {
				adTimeranges = strings.Replace(adTimeranges,"[","",-1)
				adTimeranges = strings.Replace(adTimeranges,"]","",-1)
				hourRange := strings.Split(adTimeranges, "-")
				startHour := util.StringToInt(hourRange[0][0:2])
				endHour := util.StringToInt(hourRange[1][0:2])
				if nowHour >= startHour && nowHour < endHour {
					inTimeRangesCheck = 1
				}
			}

			hourLimitCheck := 0
			if CheckHourLimit(adv, appId) {
				hourLimitCheck = 1
			}

			checkMap["statusCheck"] = util.IntToString(status)
			checkMap["balanceCheck"] = util.IntToString(balanceCheck)
			checkMap["inTimeRangesCheck"] = util.IntToString(inTimeRangesCheck)
			checkMap["hourLimitCheck"] = util.IntToString(hourLimitCheck)

			cpmResult[util.AD_DM_SENSEAR_AD_REALTIME_CPM+adId] = checkMap

		}

		if USE_REDIS == 1 {
			redisClient.HmsetByPipeline(util.REDIS_DM,util.REDIS_DB_DM,cptResult,realtime_task_fre*60)
			redisClient.HmsetByPipeline(util.REDIS_DM,util.REDIS_DB_DM,cpmResult,realtime_task_fre*60)
		}else {
			for k,v := range cptResult{
				buntDBclient.WriteMap(k,v,util.CPT_ADSTAT_DB)
			}
			for k,v := range cpmResult{
				buntDBclient.WriteMap(k,v,util.CPM_ADSTAT_DB)
			}
		}

	}
}

func OfflinePreloadProcess(){
	appIdList := GetAllApps()

	task_fre := util.StringToInt(util.NewConfigHelper().ConfigMap["PRE_LOAD_TASK_PRE"])
	for _,appid := range appIdList.Elements(){
		appId := util.InterfaceToString(appid)
		adList := GetMediaAllAds(appId)

		if len(adList) == 0 {
			continue
		}

		result := make(map[string]([]string))
		model2result := make(map[string]([]string))
		targetCpmModel2Advs := make(map[string]([](map[string]string)))
		model5result := make(map[string][]string)
		targetCpmModel5Advs := make(map[string]([](map[string]string)))

		cptAdList := make([](map[string]string),0)
		for _,adver := range adList{
			if fpCPTPredicate(adver,1) {
				cptAdList = append(cptAdList,adver)
			}
		}

		for _,adv := range cptAdList{
			aAudienceOss := util.StringToListInt(adv["audience_oss"])
			adMode := adv["ad_mode"]
			if "2" == adMode {
				if len(aAudienceOss) > 0{
					for _,os := range aAudienceOss{
						adCons := util.AD_DM_SENSEAR_AD_PRELOAD+appId+":"+util.IntToString(os)
						if _,ok := model2result[adCons]; ok{
							model2result[adCons] = append(model2result[adCons],adv["advertisement_id"])
						}else {
							advIdList := make([]string,0)
							advIdList = append(advIdList,adv["advertisement_id"])
							model2result[adCons] = advIdList
						}
					}
				}else {
					adCons := util.AD_DM_SENSEAR_AD_PRELOAD+appId+":-1"
					if _,ok := model2result[adCons]; ok{
						model2result[adCons] = append(model2result[adCons],adv["advertisement_id"])
					}else {
						advIdList := make([]string,0)
						advIdList = append(advIdList,adv["advertisement_id"])
						model2result[adCons] = advIdList
					}
				}
			}else if "5" == adMode{
				if len(aAudienceOss) > 0{
					for _,os := range aAudienceOss{
						adCons := util.AD_DM_SENSEAR_AD_PRELOAD+appId+":"+util.IntToString(os)
						if _,ok := model5result[adCons]; ok{
							model5result[adCons] = append(model5result[adCons],adv["advertisement_id"])
						}else {
							advIdList := make([]string,0)
							advIdList = append(advIdList,adv["advertisement_id"])
							model5result[adCons] = advIdList
						}
					}
				}else {
					adCons := util.AD_DM_SENSEAR_AD_PRELOAD+appId+":-1"
					if _,ok := model5result[adCons]; ok{
						model5result[adCons] = append(model5result[adCons],adv["advertisement_id"])
					}else {
						advIdList := make([]string,0)
						advIdList = append(advIdList,adv["advertisement_id"])
						model5result[adCons] = advIdList
					}
				}
			}
		}

		//处理CPM广告
		cpmAdList := make([](map[string]string),0)
		for _,adver := range adList{
			if fpCPMPredicate(adver,1) {
				cpmAdList = append(cpmAdList,adver)
			}
		}

		AddAdExInfo(cpmAdList)
		dailyMax := 0.0
		priceMax := 0.0
		
		for _,adv := range cpmAdList{
			aAudienceGenders := util.StringToListInt(adv["audience_genders"])
			aAudienceAgeGroups := util.StringToListInt(adv["audience_agegroups"])
			aAudienceOss := util.StringToListInt(adv["audience_oss"])
			aAudienceAreas := []int{-1}
			if _,ok := adv["areas"];ok {
				aAudienceAreas = util.StringToListInt(adv["areas"])
			}
			adMode := adv["ad_mode"]

			adDaily := util.GetDoubleValueFromMap(adv,"daily_min",0.0)
			if adDaily > dailyMax {
				dailyMax = adDaily
			}

			price := util.GetDoubleValueFromMap(adv,"actual_price",0.0);
			if price > priceMax{
				priceMax = price
			}

			for _,gender := range aAudienceGenders{
				for _,ageGroup := range aAudienceAgeGroups{
					for _,os := range aAudienceOss{
						for _,area := range aAudienceAreas{
							adCons := util.AD_DM_SENSEAR_AD_PRELOAD+appId+":"+util.IntToString(gender)+"_"+util.IntToString(ageGroup)+"_"+util.IntToString(os)+"_"+util.IntToString(area)
							if "2" == adMode {
								if _,ok := targetCpmModel2Advs[adCons];ok{
									targetCpmModel2Advs[adCons] = append(targetCpmModel2Advs[adCons],adv)
								}else {
									advList := make([](map[string]string),0)
									advList = append(advList,adv)
									targetCpmModel2Advs[adCons] = advList
								}
							}else if "5" == adMode{
								if _,ok := targetCpmModel5Advs[adCons];ok{
									targetCpmModel5Advs[adCons] = append(targetCpmModel5Advs[adCons],adv)
								}else {
									advList := make([](map[string]string),0)
									advList = append(advList,adv)
									targetCpmModel5Advs[adCons] = advList
								}
							}
						}
					}
				}
			}
		}

		//排序
		for targetKey,_ := range targetCpmModel2Advs{
			advList := targetCpmModel2Advs[targetKey]
			for _,adv := range advList{
				GetSortX(adv,dailyMax,priceMax)
			}
			advList = Sort(advList)
			adIdList := make([]string,0)
			for _,adInfo := range advList{
				adIdList = append(adIdList,adInfo["advertisement_id"]+"_"+adInfo["sort"])
			}
			model2result[targetKey] = adIdList
		}

		for targetKey,_ := range targetCpmModel5Advs{
			advList := targetCpmModel5Advs[targetKey]
			for _,adv := range advList{
				GetSortX(adv,dailyMax,priceMax)
			}
			advList = Sort(advList)
			adIdList := make([]string,0)
			for _,adInfo := range advList{
				adIdList = append(adIdList,adInfo["advertisement_id"]+"_"+adInfo["sort"])
			}
			model5result[targetKey] = adIdList
		}

		//截取列表的前10个元素
		for key,_ := range model2result{
			value := model2result[key]
			if len(value) < 10 {
				result[key] = value
			}else {
				result[key] = value[0:10]
			}
		}
		//将两个map合并
        for key,_ := range model5result{
			newValue := model5result[key]
			if _,ok := result[key]; !ok{
				if len(newValue) < 10 {
					result[key] = newValue
				}else {
					result[key] = newValue[0:10]
				}
			}else {
				originValue := result[key]
				if len(originValue) < 10 {
					for _,value := range originValue{
						originValue = append(originValue,value)
					}
				}else {
					for _,value := range originValue[0:10]{
						originValue = append(originValue,value)
					}
				}
				result[key] = originValue
			}
		}

		if USE_REDIS == 1{
			redisClient.LsetByPipeline(util.REDIS_DM,util.REDIS_DB_DM,result,60*task_fre)
		}else {
			for key,value := range result{
				buntDBclient.WriteArr(key,value,util.PRELOAD_ADINFO_DB)
			}
		}
	}
}

func getShowTimesLimit(adInfo map[string]string) int{
	showTimesLimit := -1
	freqctrlType := adInfo["freqctrl_type"]
	if freqctrlType != ""{
		showTimesLimit = util.StringToInt(adInfo["max_show_times"])
	}
	return showTimesLimit
}

func fpCPTPredicate(adver map[string]string,plusDays int) bool{
	flag := false
	costType := adver["cost_type"]
	adMode := adver["ad_mode"]

	startDate := util.GetIntValueFromMap(adver,"start_date",0)
	endDate := util.GetIntValueFromMap(adver,"end_date",0)

	atDate := false
	if endDate == 0 {
		atDate = true
	}else{
		//timeStr := time.Now().AddDate(0,0,plusDays).Format("2006-01-02")
		timeStr := time.Now().AddDate(0,0,plusDays).Format("2006-01-02")
		t, _ := time.Parse("2006-01-02", timeStr)
		todayMills := t.UnixNano()/1000000 - 8 * 60 * 60 * 1000
		if int(todayMills) >= startDate && int(todayMills)<= endDate {
			atDate = true
		}
	}
	if (costType != "" && "3" == costType) && adMode != "" && (adMode != "" && ("2" == adMode || "5" == adMode)) && atDate {
		flag = true
	}
	return flag
}

func fpCPMPredicate(adver map[string]string,plusDays int) bool {
	flag := false
	costType := adver["cost_type"]
	adMode := adver["ad_mode"]

	startDate := util.GetIntValueFromMap(adver,"start_date",0)
	endDate := util.GetIntValueFromMap(adver,"end_date",0)

	atDate := false
	if endDate == 0 {
		atDate = true
	}else{
		timeStr := time.Now().AddDate(0,0,plusDays).Format("2006-01-02")
		t, _ := time.Parse("2006-01-02", timeStr)
		todayMills := t.UnixNano()/1000000 - 8 * 60 * 60 * 1000
		if int(todayMills) >= startDate && int(todayMills)<= endDate {
			atDate = true
		}
	}
	if (costType != "" && "2" == costType) && adMode != "" && (adMode != "" && ("2" == adMode || "5" == adMode)) && atDate {
		flag = true
	}
	return flag
}