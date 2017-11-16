package model

import (
	"encoding/json"
)

//easyjson:json
type ResponseModel struct {
	Data []string   `json:"data"`
	Reason string   `json:"reason"`
	RequestId string  `json:"request_id"`
}

func (model *ResponseModel) SetData(data []string){
	model.Data = data
}

func (model *ResponseModel) SetReason(reason string){
	model.Reason = reason
}

func (model *ResponseModel) SetRequestId(requestId string){
	model.RequestId = requestId
}

func (model *ResponseModel) GetData() []string{
	return model.Data
}

func (model *ResponseModel) GetReason() string{
	return model.Reason
}

func (model *ResponseModel) GetRequest() string{
	return model.RequestId
}

func NewResponseModel() *ResponseModel{
	return &ResponseModel{}
}

func NewResponseModelWithArgs(data []string,reason string,requestId string) *ResponseModel{
	return &ResponseModel{
		Data:data,
		Reason:reason,
		RequestId:requestId,
	}
}

func (model *ResponseModel) ToString() string{
	result,err := json.Marshal(model)
	if err != nil {
		panic(err)
	}
	return string(result)
}
