package main

import 	"github.com/jolestar/go-commons-pool"
import 	"git.apache.org/thrift.git/lib/go/thrift"

type UserProcessorFactory struct {
   processorPool *pool.ObjectPool
}

func NewUserProcessorFactory() *UserProcessorFactory{
	processorPool := pool.NewObjectPoolWithDefaultConfig(NewSenseProcessFactory())
	return &UserProcessorFactory{processorPool:processorPool}
}

func (userFactory *UserProcessorFactory) GetProcessor(trans thrift.TTransport) thrift.TProcessor{
   if processor,err := userFactory.processorPool.BorrowObject(); err == nil{
	   if pro,ok := processor.(thrift.TProcessor); ok {
		   return pro
	   }else {
		   return nil
	   }
   }else {
	   return nil
   }
}

func (userFactory *UserProcessorFactory) ReturnProcessor(trans thrift.TTransport) {
    userFactory.processorPool.ReturnObject(trans)
}


