package model

import (
	"encoding/json"
	"github.com/tidwall/cast"
)

type ResponseModel struct {
	data []string
	reason string
	requestId string
}

func (model *ResponseModel) SetData(data []string){
	model.data = data
}

func (model *ResponseModel) SetReason(reason string){
	model.reason = reason
}

func (model *ResponseModel) SetRequestId(requestId string){
	model.requestId = requestId
}

func (model *ResponseModel) GetData() []string{
	return model.data
}

func (model *ResponseModel) GetReason() string{
	return model.reason
}

func (model *ResponseModel) GetRequest() string{
	return model.requestId
}

func NewResponseModel() *ResponseModel{
	return &ResponseModel{}
}

func NewResponseModelWithArgs(data []string,reason string,requestId string) *ResponseModel{
	return &ResponseModel{
		data:data,
		reason:reason,
		requestId:requestId,
	}
}

func ModelToString(model ResponseModel) string{
	result,err := json.Marshal(model)
	if err != nil {
		panic(err)
	}
	return cast.ToString(result)
}