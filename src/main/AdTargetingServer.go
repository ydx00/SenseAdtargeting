package main

/**
  rpc服务框架
*/
import (
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"os"
	"service"
	"thrift_service"
)

const (
	NetworkAddr = "127.0.0.1:19090"
)

func main() {
	//startServer()

}

func startServer() {
	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	serverTransport, err := thrift.NewTServerSocket(NetworkAddr)
	if err != nil {
		fmt.Println("Error!", err)
		os.Exit(1)
	}

	handler := &service.TargetingServiceImpl{}
	processor := thrift_service.NewAdTargetingServiceProcessor(handler)
	server := thrift.NewTSimpleServer4(processor, serverTransport, transportFactory, protocolFactory)

	fmt.Println("thrift server in", NetworkAddr)
	server.Serve()
}
