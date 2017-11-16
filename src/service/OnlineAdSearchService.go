package service

import (
	"util"
	"log"
	"set"
	"time"
	"math/rand"
	"model"
	"strings"
	"sort"
)


func AdSearch(appId string,userId string,broadcasterId string,adMode int,requestId string,version int) string{
    redisclient := util.NewRedisClient()
    buntDBClient := util.GetBuntDBInstance()

    res := model.NewResponseModel()
    res.SetRequestId(requestId)

	startTime := time.Now().UnixNano()/1000000

	conditions := set.NewSimpleSet()
	fansConditions := make([]string,0)
	cptConditions := set.NewSimpleSet()
	cptCondition := util.AD_DM_SENSEAR_AD_STATIC_CPT + appId + ":" + util.IntToString(adMode)

	st1 := time.Now().UnixNano() / 1000000
	fansInfo := redisclient.HGetAll(util.REDIS_BDP_REALTIME,util.REDIS_DB_BDP_REALTIME,util.AD_BDP_SENSEAR_USER_INFO + appId + ":" + userId)
	log.Println("{\"requestId\":"+requestId+",\"info\":\"取用户画像时长===="+util.Int64ToSting(time.Now().UnixNano()/1000000-st1)+"\"}")

	if len(fansInfo) == 0 {
		log.Println("{\"requestId\":"+requestId+",\"info\":\"未能找到对应的粉丝信息的userId===="+userId+"\"}")
		cptCondition = cptCondition+":-1"
		cptConditions.Add(cptCondition)
	}else {
		os := util.GetIntValueFromMap(fansInfo,"os",-1)
		cptConditions.Add(cptCondition + ":" + util.IntToString(os))
		cptConditions.Add(cptCondition + ":-1")
	}

	st2 := time.Now().UnixNano() / 1000000
	fpCPTAds := set.NewSimpleSet()
	if USE_REDIS == 1 {
		fpCPTAds = redisclient.LGetAllTargetAdWithPipeLine(util.REDIS_DB_DM,cptConditions)
	}else {
        for _,cptCon := range cptConditions.Elements(){
			eachResult,err := buntDBClient.ReadArr(util.InterfaceToString(cptCon),util.CPT_ADINFO_DB)
			if err != nil {
				log.Fatal("查询数据库失败",err)
			}
			if len(eachResult) > 0 {
				for _,value := range eachResult{
					fpCPTAds.Add(value)
				}
			}
		}
	}
	log.Println("{\"requestId\":"+requestId+",\"info\":\"从存储取CPT广告时长===="+util.Int64ToSting(time.Now().UnixNano()/1000000-st2)+"毫秒\"}")

    st3 := time.Now().UnixNano()/1000000
	if fpCPTAds.Len() > 0{
		log.Println("{\"requestId\":"+requestId+",\"info\":\"fpCPTAds.size()===="+util.IntToString(fpCPTAds.Len())+"\"}")
		availableFpCPTAds := make([]string,0)
		adStatMap := make(map[string]string)
        for _,id := range fpCPTAds.Elements(){
        	adId := util.InterfaceToString(id)
			if USE_REDIS == 1{
				adStatMap = redisclient.HGetAll(util.REDIS_DM,util.REDIS_DB_DM,util.AD_DM_SENSEAR_AD_REALTIME_CPT + adId)
			} else{
				var err error
				adStatMap,err = buntDBClient.ReadMap(util.AD_DM_SENSEAR_AD_REALTIME_CPT +adId,util.CPT_ADSTAT_DB)
				if err != nil {
					log.Fatal("读取数据库数据失败")
					continue
				}
				if len(adStatMap) > 0{
					statusCheck := adStatMap["statusCheck"]
					if "1" == statusCheck {
						availableFpCPTAds = append(availableFpCPTAds,adId)
					}else {
						log.Println("{\"requestId\":"+requestId+",\"info\":\"该CPT广告未通过有效性校验，广告ID为===="+adId+"\"}")
						continue
					}
				}else {
					log.Println("{\"requestId\":"+requestId+",\"info\":\"未取到CPT广告的状态缓存，广告ID为===="+adId+"\"}")
					continue
				}
			}
		}

	admodenum,err := buntDBClient.ReadInt(util.AD_MODE_CPT_AD_NUM+":"+appId+":"+util.IntToString(adMode),util.ADMODE_NUM_DB)
	var randomNum int
	if err != nil  || admodenum == -1{
		randomNum = rand.Intn(3)
	}else {
		log.Println("admode_num读取数据库成功，admode_num:",admodenum)
		randomNum = rand.Intn(admodenum)
	}

	if randomNum > len(availableFpCPTAds) - 1 {
		log.Println("{\"requestId\":"+requestId+",\"info\":\"验证CPT广告有效性时长===="+string(time.Now().UnixNano()/1000000-st3)+"\"}")
	}else {
		choosenAdId := availableFpCPTAds[randomNum]
		log.Println("{\"requestId\":"+requestId+",\"info\":\"验证CPT广告有效性时长===="+string(time.Now().UnixNano()/1000000-st3)+"\"}")
		log.Println("{\"requestId\":"+requestId+",\"info\":\"return adId===================="+choosenAdId+"\"}")

		time := time.Now().UnixNano()/1000000-startTime
		overtime := false
		if time > 100 {
			overtime = true
		}

		dataList := make([]string,0)
		dataList = append(dataList,choosenAdId)
		res.SetData(dataList)
		log.Println("{\"requestId\":" + requestId + ",\"info\":\"接口调用结束，用时为：["+util.Int64ToSting(time)+"]毫秒\",\"overtime\":\""+util.BoolToString(overtime)+"\"}")
		if version == 0 {
            return choosenAdId
		}else {
			//return res.ToString()
			return ResponseToString(res)
		}
	}
  }else{
		log.Println("{\"requestId\":"+requestId+",\"info\":\"验证CPT广告有效性时长===="+util.Int64ToSting(time.Now().UnixNano()/1000000-st3)+"\"}")
		log.Println("{\"requestId\":"+requestId+",\"info\":\"没取到cpt广告\"}")
	}

	//CPM广告
	st4 := time.Now().UnixNano()/1000000
	cpmCondition := util.AD_DM_SENSEAR_AD_STATIC_CPM + appId + ":" + util.IntToString(adMode)
    if len(fansInfo) == 0{
		log.Println("{\"requestId\":"+requestId+",\"info\":\"未能找到对应的粉丝信息的userId===="+userId+"\"}")
		fansConditions = append(fansConditions,cpmCondition+":-1_-1_-1_-1")
	}else {
		fansConditions = append(fansConditions,cpmCondition+":-1_-1_-1_-1")
		//转换粉丝画像信息
        gender := util.GetStringFromMap(fansInfo,"gender","-1")
		ageGroup := util.GetStringFromMap(fansInfo,"age_group","-1")
		area := util.GetStringFromMap(fansInfo,"area","-1")
		os := util.GetStringFromMap(fansInfo,"os","-1")
		provinceCode := "-1"
		cityCode := "-1"
		if area != "-1" && len(area) == 6 {
			provinceCode = area[0:2] + "0000"
			cityCode = area[0:4] + "00"
			fansConditions = append(fansConditions,cpmCondition+":"+gender+"_"+ageGroup+"_"+os+"_"+provinceCode)

            fansConditions = append(fansConditions,cpmCondition+":-1_"+ageGroup+"_"+os+"_"+provinceCode)
            fansConditions = append(fansConditions,cpmCondition+":-1_"+ageGroup+"_-1_"+provinceCode)
            fansConditions = append(fansConditions,cpmCondition+":-1_-1_"+os+"_"+provinceCode)
            fansConditions = append(fansConditions,cpmCondition+":"+gender+"_-1_"+os+"_"+provinceCode)
            fansConditions = append(fansConditions,cpmCondition+":"+gender+"_-1_-1_"+provinceCode)
            fansConditions = append(fansConditions,cpmCondition+":"+gender+"_"+ageGroup+"_-1_"+provinceCode)
            fansConditions = append(fansConditions,cpmCondition+":-1_-1_-1_"+provinceCode)

            fansConditions = append(fansConditions,cpmCondition+":"+gender+"_"+ageGroup+"_"+os+"_"+cityCode)

            fansConditions = append(fansConditions,cpmCondition+":-1_"+ageGroup+"_"+os+"_"+cityCode)
            fansConditions = append(fansConditions,cpmCondition+":-1_"+ageGroup+"_-1_"+cityCode)
            fansConditions = append(fansConditions,cpmCondition+":-1_-1_"+os+"_"+cityCode)
            fansConditions = append(fansConditions,cpmCondition+":"+gender+"_-1_"+os+"_"+cityCode)
            fansConditions = append(fansConditions,cpmCondition+":"+gender+"_-1_-1_"+cityCode)
            fansConditions = append(fansConditions,cpmCondition+":"+gender+"_"+ageGroup+"_-1_"+cityCode)
            fansConditions = append(fansConditions,cpmCondition+":-1_-1_-1_"+cityCode)

            fansConditions = append(fansConditions,cpmCondition+":"+gender+"_"+ageGroup+"_"+os+"_-1")
            fansConditions = append(fansConditions,cpmCondition+":-1_"+ageGroup+"_"+os+"_-1")
			fansConditions = append(fansConditions,cpmCondition+":-1_"+ageGroup+"_-1_-1")
            fansConditions = append(fansConditions,cpmCondition+":-1_-1_"+os+"_-1")
            fansConditions = append(fansConditions,cpmCondition+":"+gender+"_-1_"+os+"_-1")
            fansConditions = append(fansConditions,cpmCondition+":"+gender+"_-1_-1_-1")
            fansConditions = append(fansConditions,cpmCondition+":"+gender+"_"+ageGroup+"_-1_-1")
            fansConditions = append(fansConditions,cpmCondition+":-1_-1_-1_-1")
		} else {
			fansConditions = append(fansConditions,cpmCondition+":"+gender+"_"+ageGroup+"_"+os+"_-1")
			fansConditions = append(fansConditions,cpmCondition+":-1_"+ageGroup+"_"+os+"_-1")
			fansConditions = append(fansConditions,cpmCondition+":-1_"+ageGroup+"_-1_-1")
			fansConditions = append(fansConditions,cpmCondition+":-1_-1_"+os+"_-1")
			fansConditions = append(fansConditions,cpmCondition+":"+gender+"_-1_"+os+"_-1")
            fansConditions = append(fansConditions,cpmCondition+":"+gender+"_-1_-1_-1")
            fansConditions = append(fansConditions,cpmCondition+":"+gender+"_"+ageGroup+"_-1_-1")
            fansConditions = append(fansConditions,cpmCondition+":-1_-1_-1_-1")
		}
	}

	st5 := time.Now().UnixNano() / 1000000
	if adMode == 2{
		broadcasterInfo := redisclient.HGetAll(util.REDIS_BDP_REALTIME,util.REDIS_DB_BDP_REALTIME,util.AD_BDP_SENSEAR_USER_INFO + appId + ":" + broadcasterId)
		log.Println("{\"requestId\":"+requestId+",\"info\":\"查询主播画像时长===="+util.Int64ToSting(time.Now().UnixNano()/1000000-st5)+"\"}")
		if len(broadcasterInfo) == 0 {
			log.Println("{\"未能找到对应的主播信息的broadcasterId\"===="+broadcasterId+"}")
			for _,fansCondition := range fansConditions{
				conditions.Add(fansCondition+"_-1")
			}
		}else {
			broadcasterTags := make([]string,0)
			tag_ids,ok := broadcasterInfo["tag_ids"]
			if ok {
               broadcasterTags = util.StringToListStr(tag_ids)
			}
			for _,fansCondition := range fansConditions{
				for _,broadcasterTag := range broadcasterTags{
					conditions.Add(fansCondition+"_"+broadcasterTag)
				}
				conditions.Add(fansCondition+"_-1")
			}
		}
	}else if adMode == 5 {
       for _,fansCondition := range fansConditions{
       	  conditions.Add(fansCondition)
	   }
	}
	log.Println("{\"requestId\":"+requestId+",\"info\":\"处理CPM广告条件时长===="+util.Int64ToSting(time.Now().UnixNano()/1000000-st4)+"\"}")

    st6 := time.Now().UnixNano()/1000000
	allAd := set.NewSimpleSet()
    if USE_REDIS == 1{
		allAd = redisclient.LGetAllTargetAdWithPipeLine(util.REDIS_DB_DM,conditions)
	}else {
		for _,condition := range conditions.Elements(){
			adList,err := buntDBClient.ReadArr(util.InterfaceToString(condition),util.CPM_ADINFO_DB)
			if err == nil && len(adList) != 0{
				for _,advertise := range adList{
					allAd.Add(advertise)
				}
			}
		}
	}
	log.Println("{\"requestId\":"+requestId+",\"info\":\"从存储查询CPM广告时长===="+util.Int64ToSting(time.Now().UnixNano()/1000000-st6)+"\"}")

    st7 := time.Now().UnixNano()/1000000
	distinctAdIdList := make([]string,0)
	for _,value := range allAd.Elements(){
		distinctAdIdList = append(distinctAdIdList, util.InterfaceToString(value))
	}
	log.Println(distinctAdIdList)
    sort.Sort(AdArraySort(distinctAdIdList))
	log.Println("{\"requestId\":"+requestId+",\"info\":\"CPM广告排序时长===="+util.Int64ToSting(time.Now().UnixNano()/1000000-st7)+"\"}")

	st8 := time.Now().UnixNano() / 1000000
	for _,adIdAndSort := range distinctAdIdList{
       adId := strings.Split(adIdAndSort,"_")[0]

       st9 := time.Now().UnixNano() / 1000000

       adStatMap := make(map[string]string,0)
		if USE_REDIS == 1{
			adStatMap = redisclient.HGetAll(util.REDIS_DM,util.REDIS_DB_DM,util.AD_DM_SENSEAR_AD_REALTIME_CPM+adId)
		}else {
			var err error
			adStatMap,err = buntDBClient.ReadMap(util.AD_DM_SENSEAR_AD_REALTIME_CPM + adId,util.CPM_ADSTAT_DB)
			if err != nil {
				log.Fatal("读取内存数据库失败")
			}
		}
		log.Println("{\"requestId\":" + requestId + ",\"info\":\"单条广告查询有效性缓存，用时为：["+util.Int64ToSting(time.Now().UnixNano()/1000000-st9)+"]毫秒\"}")

		if len(adStatMap) != 0 {
			statusCheck := adStatMap["statusCheck"]
			balanceCheck := adStatMap["balanceCheck"]
			inTimeRangesCheck := adStatMap["inTimeRangesCheck"]
			hourLimitCheck := adStatMap["hourLimitCheck"]
			if statusCheck == "1" && balanceCheck == "1" && inTimeRangesCheck == "1" && hourLimitCheck == "1"{
				if checkFrequencyCapping(adIdAndSort,userId,appId) {
					log.Println("{\"requestId\":" + requestId + ",\"info\":\"校验CPM广告有效性，用时为：["+util.Int64ToSting(time.Now().UnixNano()/1000000-st8)+"]毫秒\"}")
					log.Println("{\"requestId\":"+requestId+",\"info\":\"return adId===================="+adId+"\"}")
					overtime := false
					time := time.Now().UnixNano()/1000000 - startTime
					if time > 100 {
						overtime = true
					}
					dataList := make([]string,0)
					dataList = append(dataList,adId)
					res.SetData(dataList)
					log.Println("{\"requestId\":" + requestId + ",\"info\":\"接口调用结束，用时为：["+util.Int64ToSting(time)+"]毫秒\",\"overtime\":\""+util.BoolToString(overtime)+"\"}")
					if version == 0 {
						return adId
					}else {
						//return res.ToString()
						 return ResponseToString(res)
					}
				}else {
					log.Println("{\"requestId\":"+requestId+",\"info\":\"该广告未通过频次控制校验，广告ID为===="+adId+"\"}")
					continue
				}
			}else {
				log.Println("{\"requestId\":"+requestId+",\"info\":\"该广告未通过有效性校验，广告ID为===="+adId+"\"}")
				continue
			}
		}else {
			log.Println("{\"requestId\":"+requestId+",\"info\":\"未取到状态缓存，广告ID为===="+adId+"\"}")
			continue
		}
	}
	log.Println("{\"requestId\":" + requestId + ",\"info\":\"校验CPM广告有效性，全部无效，用时为：["+util.Int64ToSting(time.Now().UnixNano()/1000000-st8)+"]毫秒\"}")
	overtime := false
	time := time.Now().UnixNano()/1000000 - startTime
	if time > 100 {
		overtime = true
	}
	log.Println("{\"requestId\":" + requestId + ",\"info\":\"接口调用结束，用时为：["+util.Int64ToSting(time)+"]毫秒\",\"overtime\":\""+util.BoolToString(overtime)+"\"}")
    dataList := make([]string,0)
    res.SetData(dataList)
	res.SetReason("未取到符合条件的广告")
	log.Println("{\"requestId\":" + requestId + ",\"info\":\"接口调用结束，用时为：["+util.Int64ToSting(time)+"]毫秒\",\"overtime\":\""+util.BoolToString(overtime)+"\"}")
	if version == 0 {
		return ""
	}else {
		//return res.ToString()
		return ResponseToString(res)
	}
}


func checkFrequencyCapping(adIdAndSort string, userId string,appId string) bool{
	adId := strings.Split(adIdAndSort,"_")[0]
	showTimesLimitStr := strings.Split(adIdAndSort,"_")[2]
	if showTimesLimitStr == "-1" {
		return true
	}else {
		showTimesLimit := util.StringToInt(strings.Split(adIdAndSort,"_")[2])
		freqctrlInfo := redisClient.HGetAll(util.REDIS_BDP_REALTIME,util.REDIS_DB_BDP_REALTIME,util.SARA_KEY_USER_AD_SHOW+appId+":"+userId+":"+adId)
		showTimes := 0
		if len(freqctrlInfo) == 0 {
			return true
		}else {
			showTimes = util.GetIntValueFromMap(freqctrlInfo,"show_times",-1)
			if showTimes >= showTimesLimit{
				return false
			}
		}
	}
   return true
}

func ResponseToString(model *model.ResponseModel) string{
	result,err := model.MarshalJSON()
	if err != nil {
		panic(err)
	}
	return string(result)
}


