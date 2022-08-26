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
)

func NewResultTable(relation rai.ArrowRelation) *ResultTable {
	resultTable := new(ResultTable)
	resultTable.RelationID = relation.RelationID
	resultTable.Record = relation.Table
	//fmt.Println(relation.Table)

	// case json metadata
	resultTable.ColDefs = getColDefs(resultTable.RelationID)
	return resultTable
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
