package DNF

//easyjson:json
type Document struct {
   id string `json:"id"`
   conditions []string `json:"conditions"`
   score float64 `json:"score"`
   max_show_times int `json:"max_show_times"`
}

func NewDocument() *Document{
	return &Document{}
}

func NewDocumentWithParam(id string,conditions []string,score float64,max_show_times int) *Document{
	return &Document{
		id:id,
		conditions:conditions,
		score:score,
		max_show_times:max_show_times,
	}
}


func ResponseToString(model *Document) string{
	result,err := model.MarshalJSON()
	if err != nil {
		panic(err)
	}
	return string(result)
}


