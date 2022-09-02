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
	"math"
	"math/big"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow/go/v9/arrow"
	"github.com/apache/arrow/go/v9/arrow/array"
	"github.com/apache/arrow/go/v9/arrow/float16"
	"github.com/shopspring/decimal"
)

const unixEPOCH = 62135683200000
const millisPerDay = 24 * 60 * 60 * 1000
const decimalsRegex = "^FixedPointDecimals.FixedDecimal{Int([0-9]+), ([0-9]+)}$"
const rationalRegEx = "^Rational{Int([0-9]+)}$"

func convertValue(typeDef map[string]interface{}, value interface{}) (interface{}, error) {
	switch typeDef["type"] {
	case "Constant":
		return typeDef["value"].(map[string]interface{})["value"], nil
	case "String":
		return value, nil
	case "Char":
		return fmt.Sprintf("%c", int(value.(uint32))), nil
	case "Bool":
		return value, nil
	case "DateTime":
		sec, dec := math.Modf(float64(value.(int64)-unixEPOCH) / 1000.0)
		return time.Unix(int64(sec), int64(dec*(1e9))).Format(time.RFC3339), nil
	case "Date":
		ms := int64(value.(int64)*millisPerDay - unixEPOCH)
		return time.UnixMilli(ms).Format("2006-01-02"), nil
	case "Month":
		return time.Month(value.(int64)), nil
	case "Year":
		return value.(int64), nil
	case "Day":
		return value.(int64), nil
	case "Week":
		return value.(int64), nil
	case "Hour":
		return value.(int64), nil
	case "Minute":
		return value.(int64), nil
	case "Second":
		return value.(int64), nil
	case "Millisecond":
		return value.(int64), nil
	case "Microsecond":
		return value.(int64), nil
	case "Nanosecond":
		return value.(int64), nil
	case "Missing":
		return nil, nil
	case "FilePos":
		return value.(int64), nil
	case "Hash":
		return uint128ToMathInt128(value), nil
	case "UInt8":
		return value, nil
	case "UInt16":
		return value, nil
	case "UInt32":
		return value, nil
	case "UInt64":
		return value, nil
	case "UInt128":
		return uint128ToMathInt128(value), nil
	case "Int8":
		return value, nil
	case "Int16":
		return value, nil
	case "Int32":
		return value, nil
	case "Int64":
		return value, nil
	case "Int128":
		return int128ToMathInt128(value), nil
	case "Float16":
		switch value.(type) {
		case float16.Num:
			v := value.(float16.Num)
			return v.Float32(), nil
		case float32:
			return value, nil
		default:
			panic(fmt.Sprintf("unhandled Float16 type conversion %T", value))
		}
	case "Float32":
		return float32(value.(float32)), nil
	case "Float64":
		return float64(value.(float64)), nil
	case "Decimal16":
		switch value.(type) {
		case int16:
			v := int64(value.(int16))
			exp, err := strconv.Atoi(typeDef["places"].(string))
			return decimal.New(v, -int32(exp)), err
		case int32:
			v := int64(value.(int32))
			exp, err := strconv.Atoi(typeDef["places"].(string))
			return decimal.New(v, -int32(exp)), err
		default:
			panic(fmt.Sprintf("unhandled Decimal16 type conversion %T", value))
		}
	case "Decimal32":
		switch value.(type) {
		case int:
			v := int64(value.(int))
			exp, err := strconv.Atoi(typeDef["places"].(string))
			return decimal.New(v, -int32(exp)), err
		case int32:
			v := int64(value.(int32))
			exp, err := strconv.Atoi(typeDef["places"].(string))
			return decimal.New(v, -int32(exp)), err
		default:
			panic(fmt.Sprintf("unhandled Decimal32 type conversion %T", value))
		}
	case "Decimal64":
		v := int64(value.(int64))
		exp, err := strconv.Atoi(typeDef["places"].(string))
		return decimal.New(v, -int32(exp)), err
	case "Decimal128":
		v := int128ToMathInt128(value)
		exp, err := strconv.Atoi(typeDef["places"].(string))
		// FixMe: decimals doesn't support big.Int
		return decimal.New(v.Int64(), -int32(exp)), err
	case "Rational8":
		v1 := value.([]interface{})[0]
		v2 := value.([]interface{})[1]
		switch v1.(type) {
		case int8:
			v1 = int64(v1.(int8))
			v2 = int64(v2.(int8))
			return big.NewRat(v1.(int64), v2.(int64)), nil
		case int32:
			v1 = int64(v1.(int32))
			v2 = int64(v2.(int32))
			return big.NewRat(v1.(int64), v2.(int64)), nil
		default:
			panic(fmt.Sprintf("unhandled Rational8 type conversion %T", v1))
		}
	case "Rational16":
		v1 := value.([]interface{})[0]
		v2 := value.([]interface{})[1]
		switch v1.(type) {
		case int16:
			v1 = int64(v1.(int16))
			v2 = int64(v2.(int16))
			return big.NewRat(v1.(int64), v2.(int64)), nil
		case int32:
			v1 = int64(v1.(int32))
			v2 = int64(v2.(int32))
			return big.NewRat(v1.(int64), v2.(int64)), nil
		default:
			panic(fmt.Sprintf("unhandled Rational8 type conversion %T", v1))
		}
	case "Rational32":
		v1 := int64(value.([]interface{})[0].(int32))
		v2 := int64(value.([]interface{})[1].(int32))
		return big.NewRat(v1, v2), nil
	case "Rational64":
		v1 := int64(value.([]interface{})[0].(int64))
		v2 := int64(value.([]interface{})[1].(int64))
		return big.NewRat(v1, v2), nil
	case "Rational128":
		v := value.([]interface{})[0].([]interface{})
		v1 := int128ToMathInt128(v[0:2])
		v2 := int128ToMathInt128(v[2:4])
		// FIXME: big.Rat doesn't support big.Int
		return big.NewRat(v1.Int64(), v2.Int64()), nil
	case "ValueType":
		// var physicalTypeDefs []map[string]interface{}
		// for _, tp := range typeDef["typeDefs"].([]interface{}) {
		// 	if tp.(map[string]interface{})["type"] != "Constant" {
		// 		physicalTypeDefs = append(physicalTypeDefs, tp.(map[string]interface{}))
		// 	}
		// }

		physicalIndex := -1

		var values []interface{}
		for _, tp := range typeDef["typeDefs"].([]interface{}) {
			if tp.(map[string]interface{})["type"] == "Constant" {
				v, err := convertValue(tp.(map[string]interface{}), nil)
				if err != nil {
					return nil, err
				}
				values = append(values, v)
			} else {
				physicalIndex++
				vx, ok := value.([]interface{})
				if ok {
					v, err := convertValue(tp.(map[string]interface{}), vx[physicalIndex])
					if err != nil {
						return values, err
					}
					values = append(values, v)
				} else {
					vx := []interface{}{value}
					v, err := convertValue(tp.(map[string]interface{}), vx[physicalIndex])
					if err != nil {
						return values, err
					}
					values = append(values, v)
				}

			}
		}
		return values, nil

	default:
		panic(fmt.Errorf("unhandled value type %v", typeDef["type"]))
	}
	return nil, nil
}

// FIXME: can't handle negative values
func int128ToMathInt128(tuple interface{}) *big.Int {
	fmt.Println(tuple)
	switch tuple.(type) {
	case []interface{}:
		t1 := tuple.([]interface{})[0]
		t2 := tuple.([]interface{})[1]
		return new(big.Int).SetBits(
			[]big.Word{
				big.Word(t1.(uint64)),
				big.Word(t2.(uint64)),
			},
		)
	case []uint64:
		t1 := tuple.([]uint64)[0]
		t2 := tuple.([]uint64)[1]
		return new(big.Int).SetBits(
			[]big.Word{
				big.Word(t1),
				big.Word(t2),
			},
		)
	case uint64:
		v := tuple.(uint64)
		return new(big.Int).SetBits(
			[]big.Word{
				big.Word(v),
				0,
			},
		)
	default:
		panic(fmt.Sprintf("unhandled tuple type %T", tuple))
	}
}

func uint128ToMathInt128(tuple interface{}) *big.Int {
	switch tuple.(type) {
	case []interface{}:
		t1 := tuple.([]interface{})[0]
		t2 := tuple.([]interface{})[1]
		return new(big.Int).SetBits(
			[]big.Word{
				big.Word(t1.(uint64)),
				big.Word(t2.(uint64)),
			},
		)
	case []uint64:
		t1 := tuple.([]uint64)[0]
		t2 := tuple.([]uint64)[1]
		return new(big.Int).SetBits(
			[]big.Word{
				big.Word(t1),
				big.Word(t2),
			},
		)
	default:
		panic(fmt.Sprintf("unhandled tuple type %T", tuple))
	}
}

func _unmarshall(data string) (map[string]interface{}, error) {
	var typeDef map[string]interface{}
	if err := json.Unmarshal([]byte(data), &typeDef); err != nil {
		return make(map[string]interface{}), nil
	}

	return typeDef, nil
}

func getTypeDef(tp string) (map[string]interface{}, error) {
	if strings.HasPrefix(tp, ":") {
		return _unmarshall(fmt.Sprintf(`{"type":"Constant","value":{"type":"String","value":"%s"}}`, tp))
	}

	if strings.ContainsAny(tp, "(") && !strings.HasPrefix(tp, "(") {
		return _unmarshall(fmt.Sprintf(`{"type":"Constant","value":{"type":"String","value":"%s"}}`, tp))
	}

	if tp == "String" {
		return _unmarshall(`{"type": "String"}`)
	}

	if tp == "Bool" {
		return _unmarshall(`{"type":"Bool"}`)
	}

	if tp == "Char" {
		return _unmarshall(`{"type":"Char"}`)
	}

	if tp == "Dates.DateTime" {
		return _unmarshall(`{"type":"DateTime"}`)
	}

	if tp == "Dates.Date" {
		return _unmarshall(`{"type":"Date"}`)
	}

	if tp == "Dates.Year" {
		return _unmarshall(`{"type":"Year"}`)
	}

	if tp == "Dates.Month" {
		return _unmarshall(`{"type":"Month"}`)
	}

	if tp == "Dates.Week" {
		return _unmarshall(`{"type":"Week"}`)
	}

	if tp == "Dates.Day" {
		return _unmarshall(`{"type":"Day"}`)
	}

	if tp == "Dates.Hour" {
		return _unmarshall(`{"type":"Hour"}`)
	}

	if tp == "Dates.Minute" {
		return _unmarshall(`{"type":"Minute"}`)
	}

	if tp == "Dates.Second" {
		return _unmarshall(`{"type":"Second"}`)
	}

	if tp == "Dates.Millisecond" {
		return _unmarshall(`{"type":"Millisecond"}`)
	}

	if tp == "Dates.Microsecond" {
		return _unmarshall(`{"type":"Microsecond"}`)
	}

	if tp == "Dates.Nanosecond" {
		return _unmarshall(`{"type":"Nanosecond"}`)
	}

	if tp == "HashValue" {
		return _unmarshall(`{"type":"Hash"}`)
	}

	if tp == "Missing" {
		return _unmarshall(`{"type":"Missing"}`)
	}

	if tp == "FilePos" {
		return _unmarshall(`{"type":"FilePos"}`)
	}

	if tp == "Float16" {
		return _unmarshall(`{"type": "Float16"}`)
	}

	if tp == "Float32" {
		return _unmarshall(`{"type": "Float32"}`)
	}

	if tp == "Float64" {
		return _unmarshall(`{"type": "Float64"}`)
	}

	if tp == "Int8" {
		return _unmarshall(`{"type": "Int8"}`)
	}

	if tp == "Int16" {
		return _unmarshall(`{"type": "Int16"}`)
	}

	if tp == "Int32" {
		return _unmarshall(`{"type": "Int32"}`)
	}

	if tp == "Int64" {
		return _unmarshall(`{"type": "Int64"}`)
	}

	if tp == "Int128" {
		return _unmarshall(`{"type": "Int128"}`)
	}

	if tp == "UInt8" {
		return _unmarshall(`{"type": "UInt8"}`)
	}

	if tp == "UInt16" {
		return _unmarshall(`{"type": "UInt16"}`)
	}

	if tp == "UInt32" {
		return _unmarshall(`{"type": "UInt32"}`)
	}

	if tp == "UInt64" {
		return _unmarshall(`{"type": "UInt64"}`)
	}

	if tp == "UInt128" {
		return _unmarshall(`{"type": "UInt128"}`)
	}

	re := regexp.MustCompile(decimalsRegex)
	matches := re.FindStringSubmatch(tp)
	if len(matches) == 3 {
		return _unmarshall(fmt.Sprintf(`{"type":"Decimal%v","places":%v}`, matches[1], matches[2]))
	}

	re = regexp.MustCompile(rationalRegEx)
	matches = re.FindStringSubmatch(tp)
	if len(matches) == 2 {
		return _unmarshall(fmt.Sprintf(`{"type":"Rational%v"}`, matches[1]))
	}
	// TODO: add the other types
	return nil, fmt.Errorf("unhandled data type %s", tp)
}

func getColDefs(relationID string) []ColumnDef {
	var types []string
	// filter empty strings
	for _, t := range strings.Split(relationID, "/") {
		if t != "" {
			types = append(types, t)
		}
	}

	colDefs := make([]ColumnDef, 0)
	arrowIndex := 0
	for _, tp := range types {
		typeDef, err := getTypeDef(tp)
		if err != nil {
			panic(err)
		}

		colDef := new(ColumnDef)
		colDef.TypeDef = typeDef

		if typeDef["type"] != "Constant" {
			colDef.ArrowIndex = arrowIndex
			arrowIndex++
		}

		colDefs = append(colDefs, *colDef)
	}

	return colDefs
}

func isFullySpecialized(colDefs []ColumnDef) bool {
	if len(colDefs) == 0 {
		return false
	}

	for _, colDef := range colDefs {
		if colDef.TypeDef["type"] != "Constant" {
			return false
		}
	}

	return true
}

func sortedMapKeys(m map[string][]interface{}) []string {
	var keys []string
	for k, _ := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func Zip(lists ...[]interface{}) func() []interface{} {
	zip := make([]interface{}, len(lists))
	i := 0
	return func() []interface{} {
		for j := range lists {
			if i >= len(lists[j]) {
				return nil
			}
			zip[j] = lists[j][i]
		}
		i++
		return zip
	}
}

func arrowRowToValues(arrowRow []interface{}, colDefs []ColumnDef) ([]interface{}, error) {
	var row []interface{}

	for _, colDef := range colDefs {
		if colDef.TypeDef["type"] == "Constant" {
			v, err := convertValue(colDef.TypeDef, nil)
			if err != nil {
				return nil, err
			}

			row = append(row, v)
		} else {
			v, err := convertValue(colDef.TypeDef, arrowRow[colDef.ArrowIndex])
			if err != nil {
				return nil, err
			}

			row = append(row, v)
		}
	}

	return row, nil
}

func join(slice []interface{}, sep string) string {
	var out []string
	for _, v := range slice {
		out = append(out, fmt.Sprintf("%v", v))
	}

	return strings.Join(out, sep)
}

func arrowArrayToArray(arr arrow.Array) []interface{} {
	var out []interface{}
	switch arr.(type) {
	case *array.Uint8:
		listValues := arr.(*array.Uint8)
		for i := 0; i < listValues.Len(); i++ {
			out = append(out, listValues.Value(i))
		}
	case *array.Uint16:
		listValues := arr.(*array.Uint16)
		for i := 0; i < listValues.Len(); i++ {
			out = append(out, listValues.Value(i))
		}
	case *array.Uint32:
		listValues := arr.(*array.Uint32)
		for i := 0; i < listValues.Len(); i++ {
			out = append(out, listValues.Value(i))
		}
	case *array.Uint64:
		listValues := arr.(*array.Uint64)
		for i := 0; i < listValues.Len(); i++ {
			out = append(out, listValues.Value(i))
		}
	case *array.Int8:
		listValues := arr.(*array.Int8)
		for i := 0; i < listValues.Len(); i++ {
			out = append(out, listValues.Value(i))
		}
	case *array.Int16:
		listValues := arr.(*array.Int16)
		for i := 0; i < listValues.Len(); i++ {
			out = append(out, listValues.Value(i))
		}
	case *array.Int32:
		listValues := arr.(*array.Int32)
		for i := 0; i < listValues.Len(); i++ {
			out = append(out, listValues.Value(i))
		}
	case *array.Int64:
		listValues := arr.(*array.Int64)
		for i := 0; i < listValues.Len(); i++ {
			out = append(out, listValues.Value(i))
		}
	case *array.Float16:
		listValues := arr.(*array.Float16)
		for i := 0; i < listValues.Len(); i++ {
			out = append(out, listValues.Value(i))
		}
	case *array.Float32:
		listValues := arr.(*array.Float32)
		for i := 0; i < listValues.Len(); i++ {
			out = append(out, listValues.Value(i))
		}
	case *array.Float64:
		listValues := arr.(*array.Float64)
		for i := 0; i < listValues.Len(); i++ {
			out = append(out, listValues.Value(i))
		}
	case *array.String:
		listValues := arr.(*array.String)
		for i := 0; i < listValues.Len(); i++ {
			out = append(out, listValues.Value(i))
		}
	case *array.Boolean:
		listValues := arr.(*array.Boolean)
		for i := 0; i < listValues.Len(); i++ {
			out = append(out, listValues.Value(i))
		}
	case *array.FixedSizeList:
		listValues := arr.(*array.FixedSizeList).ListValues()
		out = append(out, arrowArrayToArray(listValues))
	case *array.Struct:
		values := arr.(*array.Struct)
		var inner []interface{}
		for i := 0; i < values.NumField(); i++ {
			inner = append(inner, arrowArrayToArray(values.Field(i))...)
		}
		out = append(out, inner)

	default:
		panic(fmt.Sprintf("unhandled array value type: %T", arr))
	}

	return out
}

func strToBig(s string) *big.Int {
	b := new(big.Int)
	if v, ok := b.SetString(s, 10); ok {
		return v
	} else {
		return nil
	}
}
