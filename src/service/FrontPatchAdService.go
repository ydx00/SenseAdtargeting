package service

import (
	"util"
	"time"
	"log"
	"sort"
	"fmt"
	"strings"
	"set"
)


func GetMediaAllAds(appId string) [](map[string]string){
	advertiserAdList := make([](map[string]string),0)
	advertiserAds := redisclient.LRange(util.REDIS_SENSEAR,util.REDIS_DB_SARA,util.SARA_KEY_AD_POST_DATA+appId,0,-1)
	if len(advertiserAds) > 0 {
		advertiserAdList = GetAdList(advertiserAds)
		//fmt.Println(len(advertiserAdList))
	}
	return advertiserAdList
}


func GetAdList(adIdList []string) [](map[string]string){
	return redisclient.HGetAllAdWithPipeline(util.REDIS_DB_SARA,adIdList)
}


func GetAllApps() set.Set{
	return redisclient.HGetAllApps(util.REDIS_DB_SARA)
}


func AddAdExInfo(adList [](map[string]string)){
	adIdList := make([]string,0)
	adPlanIdMap := make(map[string]string)
	advertiserIdMap := make(map[string]string)

	adNotInCache := make([](map[string]string),0)

	for _,adInfo := range adList{
		adIdList = append(adIdList,adInfo["advertisement_id"])
	}

	adExInfoMap := redisclient.HGetAllAdExInfoWhithPipeline(util.REDIS_DB_DM,adIdList)

	for _,adInfo := range adList{
		if len(adExInfoMap[adInfo["advertisement_id"]]) != 0 {
			for k,v := range adExInfoMap[adInfo["advertisement_id"]]{
				adInfo[k] = v
			}
		}else {
			adPlanIdMap[adInfo["advertisement_id"]] = adInfo["plan_id"]
			advertiserIdMap[adInfo["advertisement_id"]] = adInfo["advertiser_id"]
			adNotInCache = append(adNotInCache,adInfo)
		}
	}

	if len(adNotInCache) > 0 {
		advertiserCostMap := redisclient.HGetAllAdvertiserCostInfoWhithPipeline(util.REDIS_DB_BDP_REALTIME,advertiserIdMap)
		planCostMap := redisclient.HGetAllAdPlanCostWhithPipeline(util.REDIS_DB_BDP_REALTIME,adPlanIdMap)

		advertiserMap := redisclient.HGetAllAdvertiserInfoWhithPipeline(util.REDIS_DB_SARA,advertiserIdMap)
		planMap := redisclient.HGetAllAdPlanWhithPipeline(util.REDIS_DB_SARA,adPlanIdMap)

		for _,adInfo := range adNotInCache{
			adId := adInfo["advertisement_id"]
			dailyMin := 0.0

			adExInfoNew := make(map[string]string)

			if len(planMap) > 0 {
				adPlanMap := planMap[adId]
				adPlanCostMap := planCostMap[adId]
				//推广计划限额
				dailyLimit := util.GetDoubleValueFromMap(adPlanMap,"daily_limit",0.0)
				dailyCost := util.GetDoubleValueFromMap(adPlanCostMap,"day_cost",0.0)
				available := dailyLimit - dailyCost
				adExInfoNew["available"] = util.FloatToString(available)
				adExInfoNew["daily_limit"] = util.FloatToString(dailyLimit)

				if available > 0 && dailyLimit != 0 {
					 dailyMin = available
				}

				//检查广告计划日限额
				totalLimit := util.GetDoubleValueFromMap(adPlanMap,"total_limit",0.0)
				totalCost := util.GetDoubleValueFromMap(adPlanCostMap,"total_cost",0.0)
				totalAvailable := totalLimit - totalCost
				adExInfoNew["total_available"] = util.FloatToString(totalAvailable)
				adExInfoNew["total_limit"] = util.FloatToString(totalLimit)

				if totalAvailable > 0 && totalLimit != 0 && dailyMin > totalAvailable{
					dailyMin = totalAvailable
				}

				if len(advertiserMap) > 0 {
					//广告主余额
					advertiserInfoMap := advertiserMap[adId]
					advertiserCostInfoMap := advertiserCostMap[adId]
					amount := util.GetDoubleValueFromMap(advertiserInfoMap,"total_amount",0.0)
					cost := util.GetDoubleValueFromMap(advertiserCostInfoMap,"total_cost",0.0)
					availableBalance := amount - cost
					adExInfoNew["available_balance"] = util.FloatToString(availableBalance)

					if dailyMin == 0.0 || dailyMin > availableBalance {
						dailyMin = availableBalance
					}
				}
				adExInfoNew["daily_min"] = util.FloatToString(dailyMin)

				for k,v := range adExInfoNew{
					adInfo[k] = v
				}
				redisclient.Hmset(util.REDIS_DM,util.REDIS_DB_DM,util.SARA_KEY_AD_EX_INFO+adId,adExInfoNew)
                redisclient.Expire(util.REDIS_DM,util.REDIS_DB_DM,util.SARA_KEY_AD_EX_INFO+adId,300)
			}
		}
	}
}

func GetSortX(adInfo map[string]string,dailyMax float64,priceMax float64) float64 {
	daily := util.GetDoubleValueFromMap(adInfo, "daily_min", 0.0)

	dailyN := 0.0
	if dailyMax != 0.0 {
		dailyN = daily / dailyMax
	}

	price := util.GetDoubleValueFromMap(adInfo, "actual_price", 0.0)
	priceN := 0.0
	if priceMax != 0.0{
		priceN = price / priceMax
	}

	endDate := util.GetIntValueFromMap(adInfo, "end_date", 0)
	timeX := 0.0

	nowMills := time.Now().UnixNano() / 1000000
	if endDate != 0 && int64(endDate) < nowMills{
		timeX = 1.0
	}

	x := timeX * 0.3 + dailyN * 0.3 + priceN *4

	log.Println("{\"in sort ：x:  \":"+util.FloatToString(x)+"}")

	adInfo["sort"] = fmt.Sprintf("%f",x)

	return x
}

func Sort(adList [](map[string]string)) [](map[string]string){
	dailyMax := 0.0
	priceMax := 0.0

	for _,ad := range adList{
		adDaily := util.GetDoubleValueFromMap(ad,"daily_min",0.0)
		if adDaily > dailyMax {
			dailyMax = adDaily
		}

		price := util.GetDoubleValueFromMap(ad,"actual_price",0.0)
		if price > priceMax{
			priceMax = price
		}
	}

	dailyMaxFinal := dailyMax
	priceMaxFinal := priceMax

	for _,ad := range adList{
		ad["dailyMaxFinal"] = util.FloatToString(dailyMaxFinal)
		ad["priceMaxFinal"] = util.FloatToString(priceMaxFinal)
	}

    sort.Sort(BudgetSort(adList))
    return adList
}

func CheckHourLimit(ad map[string]string, appId string) bool{
	checkHourTime := time.Now().UnixNano() / 1000000
	flag := true

	adId := ad["advertisement_id"]
	planPostMethod := util.StringToInt(ad["plan_post_method"])

	log.Println("{\"in checkHourLimit plan_post_method\":"+util.IntToString(planPostMethod)+"}")

	if planPostMethod == 2 {
		log.Println("{\"检查匀速投放用时\":"+util.Int64ToSting(time.Now().UnixNano()/1000000-checkHourTime)+"毫秒}")
        return flag
	}

	hourRatioStr := redisclient.Get(util.REDIS_BDP_OFFLINE, util.REDIS_DB_BDP_OFFLINE, util.AD_BDP_SENSEAR_HOUR_RATIO+adId)
	if hourRatioStr == "" {
		hourRatioStr = util.NewConfigHelper().ConfigMap["hour_proportion"]
	}

	hourRatio := strings.Split(hourRatioStr, ",")
	adTimeranges := ad["timeranges"]

	nowHour := time.Now().Hour()

	var hourLimitRate = 0.0
	if "-1" == adTimeranges {
		hourLimitRate = util.StringToFloat(hourRatio[nowHour])
	}else {
		adTimeranges = strings.Replace(adTimeranges,"[","",-1)
		adTimeranges = strings.Replace(adTimeranges,"]","",-1)
		hourRange := strings.Split(adTimeranges,"-")
		startHour := util.StringToInt(hourRange[0][0:2])
		endHour := util.StringToInt(hourRange[1][0:2])-1

		allHourRatio := 0.0
		if startHour == endHour {
			allHourRatio = util.StringToFloat(hourRatio[startHour])
		}else {
			for i := startHour; i <= endHour; i++  {
				allHourRatio += util.StringToFloat(hourRatio[i])
			}
		}

		if allHourRatio == 0{
			hourLimitRate = 0.0
		}else {
			hourLimitRate = util.StringToFloat(hourRatio[nowHour]) / allHourRatio
		}
	}

	log.Println("{\"in checkHourLimit hourLimitRate\":"+util.FloatToString(hourLimitRate)+"}")
	price := util.StringToFloat(ad["actual_price"])

	dailyMin := 0.0
	daily_min := ad["daily_min"]

	if daily_min != ""{
		 dailyMin = util.StringToFloat(daily_min)
	}

	dailyShowTimesLimit := (dailyMin/price) * 1000
	hourShowTimesLimit := int64(dailyShowTimesLimit * hourLimitRate)

	log.Println("{\"in checkHourLimit hourShowTimesLimit\":"+util.Int64ToSting(hourShowTimesLimit)+"}")

	adStatisticsData := redisclient.HGetAll(util.REDIS_BDP_REALTIME, util.REDIS_DB_BDP_REALTIME,util.SARA_KEY_AD_STATISTICS_DATA + adId)

	hourShowTimes := util.GetInt64ValueFromMap(adStatisticsData, "hour_exposure_times", 0)
	hourUpdateTime := util.GetInt64ValueFromMap(adStatisticsData, "hour_update_time", 0)

	hourUpdateTimeHour := time.Unix(0, hourUpdateTime).Hour()

	if nowHour > hourUpdateTimeHour {
		hourShowTimes = 0
	}
	log.Println("{\"in checkHourLimit hourShowTimes\":"+util.Int64ToSting(hourShowTimes)+"}")

	if hourShowTimes > hourShowTimesLimit {
		flag = false
	}

	log.Println("{\"匀速投放用时\":"+util.Int64ToSting(time.Now().UnixNano()/1000000-checkHourTime)+"毫秒}")
    return flag
}
