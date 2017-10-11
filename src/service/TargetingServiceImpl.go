package service

import "fmt"

type TargetingServiceImpl struct {
}

func (targetingServiceImpl *TargetingServiceImpl) Search(appId string, userId string, broadcasterId string, requestId string) (r string, err error) {
	fmt.Printf("search")
	r = "Search"
	return
}

func (targetingServiceImpl *TargetingServiceImpl) SearchV2(appId string, userId string, broadcasterId string, adMode int32, requestId string) (r string, err error) {
	r = "SearchV2"
	return
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
