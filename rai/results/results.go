// Copyright 2022 RelationalAI, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package results

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/apache/arrow/go/v9/arrow"
	"github.com/apache/arrow/go/v9/arrow/array"
	"github.com/relationalai/rai-sdk-go/rai"
	"github.com/relationalai/rai-sdk-go/rai/pb"
)

func mapValueType(typeDef map[string]interface{}) (map[string]interface{}, error) {
	var relNames []map[string]interface{}
	for _, typeDef := range typeDef["typeDefs"].([]interface{})[0:3] {
		if typeDef.(map[string]interface{})["type"] == "Constant" &&
			typeDef.(map[string]interface{})["value"].(map[string]interface{})["type"] == "String" {
			relNames = append(relNames, typeDef.(map[string]interface{}))
		}
	}

	if len(relNames) != 3 ||
		!(relNames[0]["value"].(map[string]interface{})["value"] == "rel" &&
			relNames[1]["value"].(map[string]interface{})["value"] == "base") {
		return typeDef, nil
	}

	standardValueType := relNames[2]["value"].(map[string]interface{})["value"].(string)
	switch standardValueType {
	case "DateTime":
		return _unmarshall(fmt.Sprintf(`{"type":"%v"}`, standardValueType))
	case "Date":
		return _unmarshall(fmt.Sprintf(`{"type":"%v"}`, standardValueType))
	case "Year":
		return _unmarshall(fmt.Sprintf(`{"type":"%v"}`, standardValueType))
	case "Month":
		return _unmarshall(fmt.Sprintf(`{"type":"%v"}`, standardValueType))
	case "Week":
		return _unmarshall(fmt.Sprintf(`{"type":"%v"}`, standardValueType))
	case "Day":
		return _unmarshall(fmt.Sprintf(`{"type":"%v"}`, standardValueType))
	case "Hour":
		return _unmarshall(fmt.Sprintf(`{"type":"%v"}`, standardValueType))
	case "Minute":
		return _unmarshall(fmt.Sprintf(`{"type":"%v"}`, standardValueType))
	case "Second":
		return _unmarshall(fmt.Sprintf(`{"type":"%v"}`, standardValueType))
	case "Millisecond":
		return _unmarshall(fmt.Sprintf(`{"type":"%v"}`, standardValueType))
	case "Microsecond":
		return _unmarshall(fmt.Sprintf(`{"type":"%v"}`, standardValueType))
	case "Nanosecond":
		return _unmarshall(fmt.Sprintf(`{"type":"%v"}`, standardValueType))
	case "FilePos":
		return _unmarshall(fmt.Sprintf(`{"type":"%v"}`, standardValueType))
	case "Missing":
		return _unmarshall(fmt.Sprintf(`{"type":"%v"}`, standardValueType))
	case "Hash":
		return _unmarshall(fmt.Sprintf(`{"type":"%v"}`, standardValueType))
	case "FixedDecimal":
		typeDefs := typeDef["typeDefs"].([]interface{})
		td3 := typeDefs[3].(map[string]interface{})
		td4 := typeDefs[4].(map[string]interface{})

		if len(typeDefs) == 6 &&
			td3["type"].(string) == "Constant" &&
			td4["type"].(string) == "Constant" {
			bits := td3["value"].(map[string]interface{})["value"].(int64)
			places := td4["value"].(map[string]interface{})["value"].(int64)

			if bits == 16 || bits == 32 || bits == 64 || bits == 128 {
				return _unmarshall(fmt.Sprintf(`{"type":"Decimal%v", "places":"%v"}`, bits, places))
			}

			break
		}
	case "Rational":
		{
			typeDefs := typeDef["typeDefs"].([]interface{})
			if len(typeDefs) == 5 {
				tp := typeDefs[3].(map[string]interface{})

				switch tp["type"] {
				case "Int8":
					return _unmarshall(`{"type":"Rational8"}`)
				case "Int16":
					return _unmarshall(`{"type":"Rational16"}`)
				case "Int32":
					return _unmarshall(`{"type":"Rational32"}`)
				case "Int64":
					return _unmarshall(`{"type":"Rational64"}`)
				case "Int128":
					return _unmarshall(`{"type":"Rational128"}`)
				}
			}
		}
	}

	return typeDef, nil
}

func unflattenConstantValue(typeDef map[string]interface{}, value []*pb.PrimitiveValue) {
	var values []interface{}
	for _, arg := range value {
		values = append(values, mapPrimitiveValue(arg))
	}
}

func mapPrimitiveValue(val *pb.PrimitiveValue) interface{} {
	switch val.Value.(type) {
	case *pb.PrimitiveValue_StringVal:
		return string(val.Value.(*pb.PrimitiveValue_StringVal).StringVal)
	case *pb.PrimitiveValue_CharVal:
		return val.Value.(*pb.PrimitiveValue_CharVal).CharVal
	case *pb.PrimitiveValue_Int8Val:
		return val.Value.(*pb.PrimitiveValue_Int8Val).Int8Val
	case *pb.PrimitiveValue_Int16Val:
		return val.Value.(*pb.PrimitiveValue_Int16Val).Int16Val
	case *pb.PrimitiveValue_Int32Val:
		return val.Value.(*pb.PrimitiveValue_Int32Val).Int32Val
	case *pb.PrimitiveValue_Int64Val:
		return val.Value.(*pb.PrimitiveValue_Int64Val).Int64Val
	case *pb.PrimitiveValue_Int128Val:
		return []uint64{val.Value.(*pb.PrimitiveValue_Int128Val).Int128Val.Lowbits, val.Value.(*pb.PrimitiveValue_Int128Val).Int128Val.Highbits}
	case *pb.PrimitiveValue_Uint8Val:
		return val.Value.(*pb.PrimitiveValue_Uint8Val).Uint8Val
	case *pb.PrimitiveValue_Uint16Val:
		return val.Value.(*pb.PrimitiveValue_Uint16Val).Uint16Val
	case *pb.PrimitiveValue_Uint32Val:
		return val.Value.(*pb.PrimitiveValue_Uint32Val).Uint32Val
	case *pb.PrimitiveValue_Uint64Val:
		return val.Value.(*pb.PrimitiveValue_Uint64Val).Uint64Val
	case *pb.PrimitiveValue_Uint128Val:
		return []uint64{val.Value.(*pb.PrimitiveValue_Uint128Val).Uint128Val.Lowbits, val.Value.(*pb.PrimitiveValue_Uint128Val).Uint128Val.Highbits}
	default:
		panic(fmt.Sprintf("unhandled metadata primitive value %T", val.Value))
	}
}

func getColDefFromProtobuf(reltype *pb.RelType) (map[string]interface{}, error) {
	if reltype.Tag == pb.Kind_CONSTANT_TYPE &&
		reltype.ConstantType.Value != nil &&
		reltype.ConstantType.RelType != nil {

		typeDef, err := getColDefFromProtobuf(reltype.ConstantType.RelType)
		if err != nil {
			panic(err)
		}
		if typeDef["type"] != "ValueType" {
			var values []interface{}
			for _, arg := range reltype.ConstantType.Value.Arguments {
				values = append(values, mapPrimitiveValue(arg))
			}

			var value interface{}
			var err error
			if len(values) == 1 {
				value, err = convertValue(typeDef, values[0])
			} else {
				value, err = convertValue(typeDef, values)
			}

			if err != nil {
				panic(err)
			}

			// add value to typeDef
			typeDef["value"] = value
			return map[string]interface{}{
				"type":  "Constant",
				"value": typeDef,
			}, nil
		} else {
			fmt.Println("====> impelement else, look for me :D")
			unflattenConstantValue(typeDef, reltype.ConstantType.Value.Arguments)
		}
	}

	if reltype.Tag == pb.Kind_PRIMITIVE_TYPE {
		switch reltype.PrimitiveType {
		case pb.PrimitiveType_STRING:
			return _unmarshall(`{"type": "String"}`)
		case pb.PrimitiveType_SYMBOL:
			return _unmarshall(`{"type": "String"}`)
		case pb.PrimitiveType_CHAR:
			return _unmarshall(`{"type":"Char"}`)
		case pb.PrimitiveType_BOOL:
			return _unmarshall(`{"type":"Bool"}`)
		case pb.PrimitiveType_INT_8:
			return _unmarshall(`{"type":"Int8"}`)
		case pb.PrimitiveType_INT_16:
			return _unmarshall(`{"type":"Int16"}`)
		case pb.PrimitiveType_INT_32:
			return _unmarshall(`{"type":"Int32"}`)
		case pb.PrimitiveType_INT_64:
			return _unmarshall(`{"type":"Int64"}`)
		case pb.PrimitiveType_INT_128:
			return _unmarshall(`{"type":"Int128"}`)
		case pb.PrimitiveType_UINT_8:
			return _unmarshall(`{"type":"UInt8"}`)
		case pb.PrimitiveType_UINT_16:
			return _unmarshall(`{"type":"UInt16"}`)
		case pb.PrimitiveType_UINT_32:
			return _unmarshall(`{"type":"UInt32"}`)
		case pb.PrimitiveType_UINT_64:
			return _unmarshall(`{"type":"UInt64"}`)
		case pb.PrimitiveType_UINT_128:
			return _unmarshall(`{"type":"UInt128"}`)
		case pb.PrimitiveType_FLOAT_16:
			return _unmarshall(`{"type":"Float16"}`)
		case pb.PrimitiveType_FLOAT_32:
			return _unmarshall(`{"type":"Float32"}`)
		case pb.PrimitiveType_FLOAT_64:
			return _unmarshall(`{"type":"Float64"}`)
		}
	}

	// check if reltype.ValueType is not empty
	if reltype.Tag == pb.Kind_VALUE_TYPE {
		var typeDefs []interface{}
		for _, t := range reltype.ValueType.ArgumentTypes {
			m, err := getColDefFromProtobuf(t)
			if err != nil {
				panic(err)
			}

			typeDefs = append(typeDefs, m)
		}

		typeDef := map[string]interface{}{
			"type":     "ValueType",
			"typeDefs": typeDefs,
		}

		x, _ := mapValueType(typeDef)
		return x, nil
	}

	return _unmarshall(`{"type":"Unknown"}`)
}

func getColDefsFromProtobuf(relation pb.RelationId) []ColumnDef {
	colDefs := make([]ColumnDef, 0)

	arrowIndex := 0
	for _, relType := range relation.Arguments {
		typeDef, err := getColDefFromProtobuf(relType)
		if err != nil {
			panic(err)
		}

		colDef := new(ColumnDef)
		colDef.TypeDef = typeDef
		colDef.Metadata = *relType
		if typeDef["type"] != "Constant" {
			colDef.ArrowIndex = arrowIndex
			arrowIndex++
		}

		colDefs = append(colDefs, *colDef)
	}

	return colDefs
}

func NewResultTable(relation rai.ArrowRelation) *ResultTable {
	rs := new(ResultTable)
	rs.RelationID = relation.RelationID
	rs.Record = relation.Table

	// case json metadata
	//rs.ColDefs = getColDefs(rs.RelationID)
	//fmt.Println("==> colDefs from metadata.json: ", rs.ColDefs)
	//fmt.Println(getColDefsFromProtobuf(relation.Metadata))
	rs.ColDefs = getColDefsFromProtobuf(relation.Metadata)
	//for _, colDef := range rs.ColDefs {
	//	fmt.Println(colDef.Metadata)
	//	fmt.Println(colDef.TypeDef)
	//}
	return rs
}

func (r *ResultTable) ToArrowTable() arrow.Table {
	return array.NewTableFromRecords(
		r.Record.Schema(),
		[]arrow.Record{r.Record},
	)
}

// ToArrowArray returns the default result []arrow.Array
func (r *ResultTable) ToArrowArray() []arrow.Array {
	var out []arrow.Array
	for i := 0; i < int(r.Record.NumCols()); i++ {
		out = append(out, r.Record.Column(i))
	}

	return out
}

func (r *ResultTable) ToJson() {

}

// ToMap returns the json presentation of result in array of maps structured form
func (r *ResultTable) ToMap() (map[string][]interface{}, error) {
	out := make(map[string][]interface{})

	for i, arr := range r.Record.Columns() {
		colName := r.Record.ColumnName(i)
		values := arrowArrayToArray(arr)
		out[colName] = append(out[colName], values...)
	}

	return out, nil
}

func (r *ResultTable) ToArray() ([]interface{}, error) {
	var out []interface{}
	m, err := r.ToMap()
	if err != nil {
		return out, err
	}

	keys := sortedMapKeys(m)
	for _, k := range keys {
		out = append(out, m[k])
	}

	return out, nil
}

func (r *ResultTable) ToArrayRow() ([][]interface{}, error) {
	var out [][]interface{}
	m, err := r.ToMap()
	if err != nil {
		return out, nil
	}

	keys := sortedMapKeys(m)
	for i := 0; i < int(r.RowsCount()); i++ {
		var row []interface{}
		for _, k := range keys {
			row = append(row, m[k][i])
		}
		out = append(out, row)
	}

	return out, nil
}

func (r *ResultTable) TypeDefs() []map[string]interface{} {
	var out []map[string]interface{}
	for _, colDef := range r.ColDefs {
		out = append(out, colDef.TypeDef)
	}

	return out
}

func (r *ResultTable) RowsCount() int64 {
	return r.Record.NumRows()
}

func (r *ResultTable) ColumnsCount() int {
	return len(r.ColDefs)
}

func (r *ResultTable) ColmunAt(index int) ResultColumn {
	resCol := new(ResultColumn)
	colDef := r.ColDefs[index]
	arr := r.Record.Column(colDef.ArrowIndex)

	var length int
	if isFullySpecialized(r.ColDefs) {
		length = 1
	} else {
		length = arr.Len()
	}

	resCol.Array = arr
	resCol.Length = length
	resCol.TypeDef = colDef.TypeDef

	return *resCol
}

func (r *ResultTable) Get(index int) ([]interface{}, error) {
	if isFullySpecialized(r.ColDefs) && index == 0 {
		var row []interface{}
		for _, colDef := range r.ColDefs {
			v, err := convertValue(colDef.TypeDef, nil)
			if err != nil {
				return row, err
			}

			row = append(row, v)
		}
		return row, nil
	}

	arr, err := r.ToArrayRow()

	if err != nil {
		return make([]interface{}, 0), nil
	}

	arrowRow, err := arrowRowToValues(arr[index], r.ColDefs)
	if err != nil {
		return nil, err
	}

	return arrowRow, nil
}

func (r *ResultTable) Values() ([][]interface{}, error) {
	var out [][]interface{}

	for i := 0; i < int(r.RowsCount()); i++ {
		v, err := r.Get(i)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func (r *ResultTable) Slice(begin int64, end int64) *ResultTable {
	rec := r.Record.NewSlice(begin, end)

	resultTable := new(ResultTable)
	resultTable.ColDefs = r.ColDefs
	resultTable.Record = rec

	return resultTable
}

func (r *ResultTable) Print() {
	var headers []string
	w := tabwriter.NewWriter(os.Stdout, 1, 1, 1, ' ', tabwriter.Debug|tabwriter.AlignRight)

	for _, m := range r.ColDefs {
		if m.TypeDef["type"] == "Constant" {
			value := m.TypeDef["value"].(map[string]interface{})
			headers = append(headers, value["type"].(string))
		} else {
			headers = append(headers, m.TypeDef["type"].(string))
		}
	}

	fmt.Fprintln(w, strings.Join(headers, "\t"))

	values, _ := r.Values()
	for _, arr := range values {
		fmt.Fprintln(w, join(arr, "\t"))
	}

	w.Flush()
}

func (r *ResultTable) Physical() *ResultTable {
	out := new(ResultTable)
	out.ColDefs = r.ColDefs[1 : len(r.ColDefs)-1]
	out.Record = r.Record
	return out
}

func (r *ResultColumn) Values() ([]interface{}, error) {
	var out []interface{}
	if r.TypeDef["type"] == "Constant" {
		v, err := convertValue(r.TypeDef, nil)
		if err != nil {
			return out, nil
		}

		out = append(out, v)
		return out, nil
	}

	b, err := r.Array.MarshalJSON()
	if err != nil {
		return out, err
	}

	var arr []interface{}
	err = json.Unmarshal(b, &arr)
	if err != nil {
		return out, nil
	}

	for _, elem := range arr {
		v, err := convertValue(r.TypeDef, elem)
		if err != nil {
			return out, err
		}

		out = append(out, v)
	}
	return out, nil
}

func (r *ResultColumn) Get(index int) (interface{}, error) {
	if index < 0 || index >= r.Length {
		return nil, fmt.Errorf("index %d out of range %d", index, r.Length)
	}

	values, err := r.Values()
	if err != nil {
		return nil, err
	}

	return values[index], nil
}
