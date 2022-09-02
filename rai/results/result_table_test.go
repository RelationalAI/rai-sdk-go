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
	"testing"

	"github.com/apache/arrow/go/v9/arrow"
	"github.com/apache/arrow/go/v9/arrow/array"
	"github.com/apache/arrow/go/v9/arrow/memory"
	"github.com/relationalai/rai-sdk-go/rai/pb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/encoding/protojson"
)

// query
// def output =
// (#1, :foo, "w", :bar, 'a', 1);
// (#1, :foo, "x", :bar, 'b', 2);
// (#1, :foo, "y", :bar, 'c', 3);
// (#1, :foo, "z", :bar, 'd', 4)

const pbMetadataJson = `
{
    "arguments": [
        {
            "tag": "CONSTANT_TYPE",
            "constantType": {
                "relType": {
                    "tag": "PRIMITIVE_TYPE",
                    "primitiveType": "STRING"
                },
                "value": {
                    "arguments": [
                        {
                            "tag": "STRING",
                            "stringVal": "b3V0cHV0"
                        }
                    ]
                }
            }
        },
        {
            "tag": "CONSTANT_TYPE",
            "constantType": {
                "relType": {
                    "tag": "PRIMITIVE_TYPE",
                    "primitiveType": "INT_64"
                },
                "value": {
                    "arguments": [
                        {
                            "tag": "INT_64",
                            "int64Val": "1"
                        }
                    ]
                }
            }
        },
        {
            "tag": "CONSTANT_TYPE",
            "constantType": {
                "relType": {
                    "tag": "PRIMITIVE_TYPE",
                    "primitiveType": "STRING"
                },
                "value": {
                    "arguments": [
                        {
                            "tag": "STRING",
                            "stringVal": "Zm9v"
                        }
                    ]
                }
            }
        },
        {
            "tag": "PRIMITIVE_TYPE",
            "primitiveType": "STRING"
        },
        {
            "tag": "CONSTANT_TYPE",
            "constantType": {
                "relType": {
                    "tag": "PRIMITIVE_TYPE",
                    "primitiveType": "STRING"
                },
                "value": {
                    "arguments": [
                        {
                            "tag": "STRING",
                            "stringVal": "YmFy"
                        }
                    ]
                }
            }
        },
        {
            "tag": "PRIMITIVE_TYPE",
            "primitiveType": "CHAR"
        },
        {
            "tag": "PRIMITIVE_TYPE",
            "primitiveType": "INT_64"
        }
    ]
}
`

func makeMetadata(json string) pb.RelationId {
	var metadata pb.RelationId
	err := protojson.Unmarshal([]byte(json), &metadata)
	if err != nil {
		panic(err)
	}

	return metadata
}

func mockResultTable() *ResultTable {
	resultTable := new(ResultTable)

	resultTable.RelationID = "/Int64(1)/:foo/String/:bar/Char/Int64"

	// column defs
	colDefs := getColDefsFromProtobuf(makeMetadata(pbMetadataJson))
	resultTable.ColDefs = colDefs

	// record
	pool := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "v1", Type: arrow.BinaryTypes.String},
			{Name: "v2", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "v3", Type: arrow.PrimitiveTypes.Int64},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	b.Field(0).(*array.StringBuilder).AppendValues([]string{"w", "x", "y", "z"}, nil)
	b.Field(1).(*array.Uint32Builder).AppendValues([]uint32{97, 98, 99, 100}, nil)
	b.Field(2).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4}, nil)

	rec := b.NewRecord()
	rec.Retain()

	resultTable.Record = rec

	return resultTable
}

func TestTypesDefinition(t *testing.T) {
	var expectedTypeDefs = []ColumnDef{
		{
			map[string]interface{}{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			pb.RelType{},
			0,
		},
		{
			map[string]interface{}{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "Int64",
					"value": int64(1),
				},
			},
			pb.RelType{},
			0,
		},
		{
			map[string]interface{}{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "foo",
				},
			},
			pb.RelType{},
			0,
		},
		{map[string]interface{}{"type": "String"}, pb.RelType{}, 0},
		{
			map[string]interface{}{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "bar",
				},
			},
			pb.RelType{},
			0,
		},
		{map[string]interface{}{"type": "Char"}, pb.RelType{}, 1},
		{map[string]interface{}{"type": "Int64"}, pb.RelType{}, 2},
	}

	table := mockResultTable()

	for i, colDef := range table.ColDefs {
		assert.Equal(t, colDef.ArrowIndex, expectedTypeDefs[i].ArrowIndex)
		assert.Equal(t, colDef.TypeDef, expectedTypeDefs[i].TypeDef)
	}

	table.Record.Release()
}

func TestRowColumnLength(t *testing.T) {
	table := mockResultTable()

	assert.Equal(t, table.RowsCount(), int(4))
	assert.Equal(t, table.ColumnsCount(), int(7))

	table.Record.Release()
}

func TestColumnAt(t *testing.T) {
	var expectedColumns = []ResultColumn{
		{
			[]interface{}{"output", "output", "output", "output"},
			map[string]interface{}{"type": "Constant", "value": map[string]interface{}{"type": "String", "value": "output"}},
			4,
		},
		{
			[]interface{}{int64(1), int64(1), int64(1), int64(1)},
			map[string]interface{}{"type": "Constant", "value": map[string]interface{}{"type": "Int64", "value": int64(1)}},
			4,
		},
		{
			[]interface{}{"foo", "foo", "foo", "foo"},
			map[string]interface{}{"type": "Constant", "value": map[string]interface{}{"type": "String", "value": "foo"}},
			4,
		},
		{
			[]interface{}{"w", "x", "y", "z"},
			map[string]interface{}{"type": "String"},
			4,
		},
		{
			[]interface{}{"bar", "bar", "bar", "bar"},
			map[string]interface{}{"type": "Constant", "value": map[string]interface{}{"type": "String", "value": "bar"}},
			4,
		},
		{
			[]interface{}{"a", "b", "c", "d"},
			map[string]interface{}{"type": "Char"},
			4,
		},
		{
			[]interface{}{int64(1), int64(2), int64(3), int64(4)},
			map[string]interface{}{"type": "Int64"},
			4,
		},
	}
	table := mockResultTable()
	for i := 0; i < table.ColumnsCount(); i++ {
		assert.Equal(t, table.ColmunAt(i), expectedColumns[i])
	}

	table.Record.Release()
}

func TestValues(t *testing.T) {
	expectedValues := [][]interface{}{
		{"output", int64(1), "foo", "w", "bar", "a", int64(1)},
		{"output", int64(1), "foo", "x", "bar", "b", int64(2)},
		{"output", int64(1), "foo", "y", "bar", "c", int64(3)},
		{"output", int64(1), "foo", "z", "bar", "d", int64(4)},
	}
	table := mockResultTable()
	values, err := table.Values()
	assert.Nil(t, err)
	assert.Equal(t, values, expectedValues)

	v := table.Get(2)
	assert.Equal(t, v, expectedValues[2])

	table.Record.Release()
}

func TestTableSlice(t *testing.T) {
	expectedValues := [][]interface{}{
		{"output", int64(1), "foo", "w", "bar", "a", int64(1)},
		{"output", int64(1), "foo", "x", "bar", "b", int64(2)},
	}
	table := mockResultTable()

	slice := table.Slice(0, 2)
	values, err := slice.Values()
	assert.Nil(t, err)
	assert.Equal(t, values, expectedValues)

	table.Record.Release()
	slice.Record.Release()
}

func TestPhysicalTable(t *testing.T) {
	expectedValues := [][]interface{}{
		{"w", "a", int64(1)},
		{"x", "b", int64(2)},
		{"y", "c", int64(3)},
		{"z", "d", int64(4)},
	}

	table := mockResultTable()
	values, err := table.Physical().Values()
	assert.Nil(t, err)
	assert.Equal(t, values, expectedValues)

}
