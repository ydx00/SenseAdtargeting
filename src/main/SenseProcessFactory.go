package main

import (
	"github.com/jolestar/go-commons-pool"
	"service"
	"thrift_service"
)

type SenseProcessFactory struct {
}

func NewSenseProcessFactory() *SenseProcessFactory{
	return &SenseProcessFactory{}
}

func (factory *SenseProcessFactory) MakeObject() (*pool.PooledObject,error){
	handler := &service.TargetingServiceImpl{}
	processor := thrift_service.NewAdTargetingServiceProcessor(handler)
	return pool.NewPooledObject(interface{}(processor)),nil
}

func (factory *SenseProcessFactory) DestroyObject(object *pool.PooledObject) error{
    return nil
}

func (factory *SenseProcessFactory) ValidateObject(object *pool.PooledObject) bool{
    return true
}

func (factory *SenseProcessFactory) ActivateObject(object *pool.PooledObject) error{
    return nil
}

func (factory *SenseProcessFactory) PassivateObject(object *pool.PooledObject) error{
    return nil
}







