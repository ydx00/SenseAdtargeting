package service

import (
	"time"
	"log"
	"util"
)

type TargetingServiceImpl struct {
}

func (targetingServiceImpl *TargetingServiceImpl) Search(appId string, userId string, broadcasterId string, requestId string) (string, error) {
	startTime := time.Now().UnixNano() / 1000000
	log.Println("{\"广告检索参数\":{\"appId:\":" + appId + ",\"userId\":" + userId + ",\"broadcasterId\":"+ broadcasterId+",\"requestId\":" + requestId + "}}")
	adId := AdSearch(appId,userId,broadcasterId,util.AD_MODE_TEN_SECOND,requestId,util.API_VERSION_OLD)
	log.Println( "{\"调用广告接口用时\":"+util.Int64ToSting(time.Now().UnixNano()/1000000-startTime)+"毫秒}")
    return adId,nil
}

func (targetingServiceImpl *TargetingServiceImpl) SearchV2(appId string, userId string, broadcasterId string, adMode int32, requestId string) (string,error) {
	startTime := time.Now().UnixNano() / 1000000
	log.Println("{\"广告检索参数\":{\"appId:\":" + appId + ",\"userId\":" + userId + ",\"broadcasterId\":"+ broadcasterId+",\"requestId\":" + requestId + "}}")
	adId := AdSearch(appId,userId,broadcasterId,int(adMode),requestId,util.API_VERSION_NEW)
	log.Println( "{\"调用广告接口用时\":"+util.Int64ToSting(time.Now().UnixNano()/1000000-startTime)+"毫秒}")
    return adId,nil
}

func (targetingServiceImpl *TargetingServiceImpl) Preload(appId string, userId string, requestId string) (r string, err error) {
	r = "Preload"
	return
}

func (targetingServiceImpl *TargetingServiceImpl) PreloadV2(appId string, userId string, adMode int32, requestId string) (r string, err error) {
	r = "PreloadV2"
	return
}

func (targetingServiceImpl *TargetingServiceImpl) UserCoverage(appId string, conditions string) (r int64, err error) {
	r = 110
	return
}
