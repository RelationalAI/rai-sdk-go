package protos

type MetadataInfoResult struct {
	Relations []Relation `json:"relations"`
}

type Relation struct {
	RelationId RelationId `json:"relationId"`
	Filename   string     `json:"fileName"`
}

type RelationId struct {
	Arguments []Argument `json:"arguments"`
}

type Argument struct {
	Tag           string       `json:"tag"`
	ConstantType  ConstantType `json:"contantType,omitempty"`
	PrimitiveType string       `json:"primitiveType,omitempty"`
	StringVal     string       `json:"stringVal,omitempty"`
}

type ConstantType struct {
	RelType RelType `json:"relType"`
	Value   Value   `json:"value"`
}

type RelType struct {
	Tag           string `json:"tag"`
	PrimitiveType string `json:"primitiveType"`
}

type Value struct {
	Arguments []Argument `json:"arguments"`
}
