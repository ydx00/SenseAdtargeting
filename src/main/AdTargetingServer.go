package main

/**
  rpc服务框架
*/
import (
	"service"
	"util"
	"time"
)

const (
	NetworkAddr = "127.0.0.1:19090"
)

func main() {
	//startServer()
	//int requestRandomNum = new Random().nextInt(100);
	//String requestId = System.currentTimeMillis()+"_"+requestRandomNum;
	requestId := string(time.Now) + "_" + "99"
    service.Search("app", "fans1", "broadcaster1",2,requestId,util.API_VERSION_OLD)
}

//func startServer() {
//	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
//	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
//
//	serverTransport, err := thrift.NewTServerSocket(NetworkAddr)
//	if err != nil {
//		fmt.Println("Error!", err)
//		os.Exit(1)
//	}
//
//	handler := &service.TargetingServiceImpl{}
//	processor := thrift_service.NewAdTargetingServiceProcessor(handler)
//	server := thrift.NewTSimpleServer4(processor, serverTransport, transportFactory, protocolFactory)
//
//	fmt.Println("thrift server in", NetworkAddr)
//	server.Serve()
//}
