package service

import (
	"util"
	"fmt"
	"log"
	"set"
	"time"
	"math/rand"
	"model"
	"encoding/json"
	"strconv"
)
const(
	USE_REDIS = 1
)
func Search(appId string,userId string,broadcasterId string,adMode int,requestId string,version int) string{
    redisclient := util.NewRedisClient()
    buntDBClient := util.GetBuntDBInstance()

    res := model.NewResponseModel()
    res.SetRequestId(requestId)

	startTime := time.Now().UnixNano()/1000

	conditions := set.NewHashSet()
	fansConditions := []string{}
	cptConditions := set.NewHashSet()
	cptCondition := fmt.Sprintf(util.AD_DM_SENSEAR_AD_STATIC_CPT,appId,":",adMode)

	st1 := time.Now().UnixNano() / 1000
	fansInfo := redisclient.HGetAll(util.REDIS_BDP_REALTIME,util.REDIS_DB_BDP_REALTIME,util.AD_BDP_SENSEAR_USER_INFO + appId + ":" + userId)
	log.Println("{\"requestId\":"+requestId+",\"info\":\"取用户画像时长===="+string(time.Now().UnixNano()/1000-st1)+"\"}")

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
	st2 := time.Now().UnixNano() / 1000
	fpCPTAds := set.NewSimpleSet()
	if USE_REDIS == 1 {
		fpCPTAds = redisclient.LGetAllTargetAdWithPipeLine(util.REDIS_DB_DM,cptConditions)
	}else {
        for _,cptCon := range cptConditions.Elements(){
			eachResult,err := buntDBClient.ReadArr(string(cptCon),util.CPT_ADINFO_DB)
			if err != nil {
				log.Fatal("查询数据库失败",err)
			}
			for _,value := range eachResult{
				fpCPTAds.Add(value)
			}
		}
	}
	log.Println("{\"requestId\":"+requestId+",\"info\":\"从存储取CPT广告时长===="+string(time.Now().UnixNano()/1000-st2)+"毫秒\"}")

    st3 := time.Now().UnixNano()/1000
	if fpCPTAds.Len() > 0{
		log.Println("{\"requestId\":"+requestId+",\"info\":\"fpCPTAds.size()===="+string(fpCPTAds.Len())+"\"}")
		availableFpCPTAds := []string{}
		var adStatMap map[string]string = nil
        for _,adId := range fpCPTAds.Elements(){
			if USE_REDIS == 1{
				adStatMap = redisclient.HGetAll(util.REDIS_DM,util.REDIS_DB_DM,util.AD_DM_SENSEAR_AD_REALTIME_CPT + string(adId))
			} else{
				var err error
				adStatMap,err = buntDBClient.ReadMap(util.AD_DM_SENSEAR_AD_REALTIME_CPT + string(adId),util.CPT_ADSTAT_DB)
				if err != nil {
					log.Fatal("读取数据库数据失败")
					continue
				}
				if adStatMap != nil{
					statusCheck := adStatMap["statusCheck"]
					if "1" == statusCheck {
						availableFpCPTAds = append(availableFpCPTAds,string(adId))
					}else {
						log.Println("{\"requestId\":"+requestId+",\"info\":\"该CPT广告未通过有效性校验，广告ID为===="+string(adId)+"\"}")
						continue
					}
				}else {
					log.Println("{\"requestId\":"+requestId+",\"info\":\"未取到CPT广告的状态缓存，广告ID为===="+string(adId)+"\"}")
					continue
				}
			}
		}

	admodenum,err := buntDBClient.ReadInt(util.AD_MODE_CPT_AD_NUM+":"+appId+":"+string(adMode),util.ADMODE_NUM_DB)
	if err != nil  || admodenum == -1{
		admodenum = rand.New(rand.NewSource(3)).Int()
	}
	var randomNum = admodenum
	if randomNum > len(availableFpCPTAds) - 1 {
		log.Println("{\"requestId\":"+requestId+",\"info\":\"验证CPT广告有效性时长===="+string(time.Now().UnixNano()/1000-st3)+"\"}")
	}else {
		choosenAdId := availableFpCPTAds[randomNum]
		log.Println("{\"requestId\":"+requestId+",\"info\":\"验证CPT广告有效性时长===="+string(time.Now().UnixNano()/1000-st3)+"\"}")
		log.Println("{\"requestId\":"+requestId+",\"info\":\"return adId===================="+choosenAdId+"\"}")

		time := time.Now().UnixNano()/1000-startTime
		overtime := false
		if time > 100 {
			overtime = true
		}

		dataList := []string{}
		dataList = append(dataList,choosenAdId)
		res.SetData(dataList)
		log.Println("{\"requestId\":" + requestId + ",\"info\":\"接口调用结束，用时为：["+string(time)+"]毫秒\",\"overtime\":\""+string(overtime)+"\"}")
		if version == 0 {
            return choosenAdId
		}else {
            return string(json.Marshal(res))
		}
	}
  }else{
		log.Println("{\"requestId\":"+requestId+",\"info\":\"验证CPT广告有效性时长===="+string(time.Now().UnixNano()/1000-st3)+"\"}")
		log.Println("{\"requestId\":"+requestId+",\"info\":\"没取到cpt广告\"}")
	}

	//CPM广告
	st4 := time.Now().UnixNano()/1000
	cpmCondition := util.AD_DM_SENSEAR_AD_STATIC_CPM + appId + ":" + string(adMode)
    if len(fansInfo) == 0{
		log.Println("{\"requestId\":"+requestId+",\"info\":\"未能找到对应的粉丝信息的userId===="+userId+"\"}")
		fansConditions = append(fansConditions,cpmCondition+":-1_-1_-1_-1")
	}else {
		fansConditions = append(fansConditions,cpmCondition+":-1_-1_-1_-1")
		//转换粉丝画像信息
        gender := getStringFromMap(fansInfo,"gender","-1")
		ageGroup := getStringFromMap(fansInfo,"age_group","-1")
		area := getStringFromMap(fansInfo,"area","-1")
		os := getStringFromMap(fansInfo,"os","-1")
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

	st5 := time.Now().UnixNano() / 1000
	if adMode == 2{
		broadcasterInfo := redisclient.HGetAll(util.REDIS_BDP_REALTIME,util.REDIS_DB_BDP_REALTIME,util.AD_BDP_SENSEAR_USER_INFO + appId + ":" + broadcasterId)
		log.Println("{\"requestId\":"+requestId+",\"info\":\"查询主播画像时长===="+string(time.Now().UnixNano()/1000-st5)+"\"}")
		if len(broadcasterInfo) == 0 {
			log.Println("{\"未能找到对应的主播信息的broadcasterId\"===="+broadcasterId+"}")
			for _,fansCondition := range fansConditions{
				conditions.Add(fansCondition+"_-1")
			}
		}else {
			//List<String> broadcasterTags = Objects.nonNull(broadcasterInfo.get("tag_ids")) ?
			//StringUtil.stringToListStr(broadcasterInfo.get("tag_ids")) :
			//Lists.newArrayList();
			//for(String fansCondition : fansConditions){
			//for(String broadcasterTag : broadcasterTags){
			//conditions.add(fansCondition+"_"+broadcasterTag);
			//}
			//conditions.add(fansCondition+"_-1");
			//}
			//var broadcasterTags []string
			//tag_ids,ok := broadcasterInfo["tag_ids"]
			//if ok {
             //  broadcasterTags =
			//}
		}
	}

}

func getStringFromMap(dict map[string]string,key string,default_value string) string{
	if value,ok := dict[key]; ok {
		return value
	}else {
		return default_value
	}
}