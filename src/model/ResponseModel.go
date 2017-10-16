package model

type ResponseModel struct {
	data []string
	reason string
	requestId string
}

func (model *ResponseModel) setData(data []string){
	model.data = data
}

func (model *ResponseModel) setReason(reason string){
	model.reason = reason
}

func (model *ResponseModel) setRequestId(requestId string){
	model.requestId = requestId
}

func (model *ResponseModel) getData() []string{
	return model.data
}

func (model *ResponseModel) getReason() string{
	return model.reason
}

func (model *ResponseModel) getRequest() string{
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