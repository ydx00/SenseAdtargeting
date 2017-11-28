package main

import (
	"util"
	"service"
	"git.apache.org/thrift.git/lib/go/thrift"
	"log"
	"github.com/robfig/cron"
	"thrift_service"
)


var SERVER_PORT = util.NewConfigHelper().ConfigMap["THRIFT_SERVER_PORT"]

var AD_STATIC_INFO_JOB_TIME = util.NewConfigHelper().ConfigMap["AD_STATIC_INFO_JOB_TIME"]
var AD_REALTIME_INFO_JOB_TIME = util.NewConfigHelper().ConfigMap["AD_REALTIME_INFO_JOB_TIME"]
var PRE_LOAD_JOB_TIME = util.NewConfigHelper().ConfigMap["PRE_LOAD_JOB_TIME"]

func startServer(){

	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	serverTransport, err := thrift.NewTServerSocket("127.0.0.1:"+SERVER_PORT)
	if err != nil {
		log.Fatalf("error on creating server socket : %s", err.Error())
		return
	}

	handler := &service.TargetingServiceImpl{}
	processor := thrift_service.NewAdTargetingServiceProcessor(handler)
	server := thrift.NewTSimpleServer4(processor, serverTransport, transportFactory, protocolFactory)


	log.Println("User Service servering in %s", "127.0.0.1:"+SERVER_PORT)
	if err = server.Serve(); err != nil {
		log.Fatal("User Service startup error: %s", err.Error())
	}

}

func main() {
	service.OfflineAdRealtimeInfoProcess()
	service.OfflineAdStaticInfoProcess()
	service.OfflinePreloadProcess()
	cronTask := cron.New()

	cronTask.AddFunc(AD_REALTIME_INFO_JOB_TIME, func() {
		service.OfflineAdRealtimeInfoProcess()
	})
	cronTask.AddFunc(AD_STATIC_INFO_JOB_TIME, func() {
		service.OfflineAdStaticInfoProcess()
	})
	cronTask.AddFunc(PRE_LOAD_JOB_TIME, func() {
		service.OfflinePreloadProcess()
	})
	cronTask.Start()
	startServer()
}