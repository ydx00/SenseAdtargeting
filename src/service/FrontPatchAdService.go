package service

import (
	"util"
)


func GetMediaAllAds(appId string) [](map[string]string){
	redisclient := util.NewRedisClient()
	advertiserAdList := make([](map[string]string),0)
	advertiserAds := redisclient.LRange(util.REDIS_SENSEAR,util.REDIS_DB_SARA,util.SARA_KEY_AD_POST_DATA+appId,0,-1)
	if len(advertiserAds) != 0 {
		advertiserAdList = GetAdList(advertiserAds)
	}
	return advertiserAdList
}


func GetAdList(adIdList []string) [](map[string]string){
	redisclient := util.NewRedisClient()
	return redisclient.HGetAllAdWithPipeline(util.REDIS_DB_SARA,adIdList)
}


func GetAllApps() []string{
	redisclient := util.NewRedisClient()
	return redisclient.HGetAllApps(util.REDIS_DB_SARA)
}
