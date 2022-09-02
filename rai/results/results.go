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
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/apache/arrow/go/v9/arrow"
	"github.com/apache/arrow/go/v9/arrow/array"
	"github.com/relationalai/rai-sdk-go/rai"
)

func NewResultTable(relation rai.ArrowRelation) *ResultTable {
	rs := new(ResultTable)
	rs.RelationID = relation.RelationID
	rs.Record = relation.Table
	rs.ColDefs = getColDefsFromProtobuf(relation.Metadata)

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

func (r *ResultTable) RowsCount() int {
	return int(r.Record.NumRows())
}

func (r *ResultTable) ColumnsCount() int {
	return len(r.ColDefs)
}

func (r *ResultTable) Columns() ([]ResultColumn, error) {
	var out []ResultColumn
	values, err := r.Values()
	if err != nil {
		return out, err
	}

	i := 0
	iter := Zip(values...)
	for tuple := iter(); tuple != nil; tuple = iter() {
		resultColumn := new(ResultColumn)
		var arr []interface{}
		for _, v := range tuple {
			arr = append(arr, v)
		}
		resultColumn.Array = arr
		if isFullySpecialized(r.ColDefs) {
			resultColumn.Length = 1
		} else {
			resultColumn.Length = len(arr)
		}

		resultColumn.TypeDef = r.ColDefs[i].TypeDef
		out = append(out, *resultColumn)
		i++
	}

	return out, nil
}

func (r *ResultTable) ColmunAt(index int) ResultColumn {
	if index >= r.ColumnsCount() {
		panic(fmt.Sprintf("index out of range [%d] with length %d", index, r.ColumnsCount()))
	}

	column, err := r.Columns()
	if err != nil {
		panic(err)
	}

	return column[index]
}

func (r *ResultTable) Get(index int) []interface{} {
	if isFullySpecialized(r.ColDefs) && index == 0 {
		var row []interface{}
		for _, colDef := range r.ColDefs {
			v, err := convertValue(colDef.TypeDef, nil)
			if err != nil {
				panic(err)
			}

			row = append(row, v)
		}
		return row
	}

	arr, err := r.ToArrayRow()

	if err != nil {
		panic(err)
	}

	arrowRow, err := arrowRowToValues(arr[index], r.ColDefs)
	if err != nil {
		panic(err)
	}

	return arrowRow
}

func (r *ResultTable) Values() ([][]interface{}, error) {
	var out [][]interface{}

	if isFullySpecialized(r.ColDefs) {
		var rows []interface{}
		for _, colDef := range r.ColDefs {
			v, err := convertValue(colDef.TypeDef, nil)
			if err != nil {
				return out, nil
			}

			rows = append(rows, v)
		}

		out = append(out, rows)
	}

	arr, err := r.ToArrayRow()

	if err != nil {
		return nil, nil
	}

	for _, value := range arr {
		v, err := arrowRowToValues(value, r.ColDefs)
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
	//out.ColDefs = r.ColDefs[1 : len(r.ColDefs)-1]
	for _, colDef := range r.ColDefs {
		if colDef.TypeDef["type"] != "Constant" {
			out.ColDefs = append(out.ColDefs, colDef)
		}
	}
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

	out = append(out, r.Array...)
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

func ShowIO(io io.Writer, tx *rai.TransactionAsyncResult) {
	for _, r := range tx.Results {
		table := NewResultTable(r)
		values, err := table.Values()
		if err != nil {
			panic(err)
		}

		for _, arr := range values {
			for j, v := range arr {
				if j > 0 {
					fmt.Fprint(io, ", ")
				}

				fmt.Fprint(io, v)
			}
			fmt.Fprintln(io)
		}
	}
}

func Show(tx *rai.TransactionAsyncResult) {
	ShowIO(os.Stdout, tx)
}
