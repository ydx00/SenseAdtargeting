package service

import (
	"log"
	"util"
	"time"
	"encoding/json"
	"strconv"
	"strings"
)

var redisclient = util.NewRedisClient()
var buntDBclient = util.GetBuntDBInstance()

func OfflineAdStaticInfoProcess(){
	appIdList := GetAllApps()
	log.Println("adList.size:"+util.IntToString(len(appIdList)))

	ad_static_task_fre := util.StringToInt(util.NewConfigHelper().ConfigMap["AD_STATIC_INFO_TASK_FRE"])

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
		cptAdList := make([](map[string]string),0)
		for _,adver := range adList{
			if fpCPTPredicate(adver,0) {
				cptAdList = append(cptAdList,adver)
			}
		}

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
			//redisClient.lsetByPipeline(RedisClient.REDIS_DM,Constant.REDIS_DB_DM,cptResult,task_fre*60);
			redisclient.LsetByPipeline(util.REDIS_DM,util.REDIS_DB_DM,cptResult,ad_static_task_fre*60)
		}else{
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
				adIdList = append(adIdList,adInfo["advertisement_id"]+"_"+adInfo["sort"]+"_"+string(getShowTimesLimit(adInfo)))
			}
			cpmResult[targetKey] = adIdList
		}

		if USE_REDIS == 1{
			redisclient.LsetByPipeline(util.REDIS_DM,util.REDIS_DB_DM,cpmResult,ad_static_task_fre*60)
		}else {
            for k,v := range cpmResult{
            	buntDBclient.WriteArr(k,v,util.CPM_ADINFO_DB)
			}
		}
		//处理广告模式CPT_NUM
		result := redisclient.HGetAll("SENSEAR",10,"SARA_KEY_APP_SIGNKEY:"+appId)

		if len(result) > 0 {
			ad_config := result["ad_configuration"]
			log.Println("AppId:"+appId)
			log.Println("广告配置信息："+ ad_config)
			if ad_config != ""{
				var returnData map[string]interface{}
				if err := json.Unmarshal([]byte(ad_config), &returnData); err == nil {
					//处理广告模式2
					if _,ok := returnData["2"]; ok {
						admode2data := returnData["2"]
						if value,ok := admode2data.(map[string]interface{}); ok{
							if len(value) > 0 {
								if _,ok := value["cpt_ad_num"]; ok{
									if admode2num,flag := (value["cpt_ad_num"]).(float64);flag && admode2num > 0{
										key := util.AD_MODE_CPT_AD_NUM + ":" + appId + ":2"
										buntDBclient.WriteInt(key,int(admode2num),util.ADMODE_NUM_DB)
									}
								}
							}
						}
					}
					//处理广告模式5
					if _,ok := returnData["5"]; ok {
						admode5data := returnData["5"]
						if value,ok := admode5data.(map[string]interface{}); ok{
							if len(value) > 0 {
								if _,ok := value["cpt_ad_num"]; ok{
									if admode5num,flag := (value["cpt_ad_num"]).(float64);flag && admode5num > 0{
										key := util.AD_MODE_CPT_AD_NUM + ":" + appId + ":5"
										buntDBclient.WriteInt(key,int(admode5num),util.ADMODE_NUM_DB)
									}
								}
							}
						}
					}
				}
			}
		}
	}
}

func OfflineAdRealtimeInfoProcess(){
	appIdList := GetAllApps()
	realtime_task_fre := util.StringToInt(util.NewConfigHelper().ConfigMap["AD_REALTIME_INFO_TASK_PRE"])
	for _,appId := range appIdList{
		adList := GetMediaAllAds(appId)
		if len(appIdList) == 0 {
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
			redisclient.HmsetByPipeline(util.REDIS_DM,util.REDIS_DB_DM,cptResult,realtime_task_fre*60)
			redisclient.HmsetByPipeline(util.REDIS_DM,util.REDIS_DB_DM,cpmResult,realtime_task_fre*60)
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
		timeStr := time.Now().AddDate(0,0,plusDays).Format("2006-01-02")
		t, _ := time.Parse("2006-01-02", timeStr)
		todayMills := t.UnixNano()/1000
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
		todayMills := t.UnixNano()/1000
		if int(todayMills) >= startDate && int(todayMills)<= endDate {
			atDate = true
		}
	}
	if (costType != "" && "2" == costType) && adMode != "" && (adMode != "" && ("2" == adMode || "5" == adMode)) && atDate {
		flag = true
	}
	return flag
}