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

package rai

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v7/arrow/memory"
	"github.com/apache/arrow/go/v9/arrow"
	"github.com/apache/arrow/go/v9/arrow/array"
	"github.com/google/uuid"
	"github.com/relationalai/rai-sdk-go/rai/pb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

var uid = uuid.New().String()

var databaseName = fmt.Sprintf("go-sdk-%s", uid)
var engineName = fmt.Sprintf("go-sdk-%s", uid)
var userEmail = fmt.Sprintf("go-sdk-%s@example.com", uid)
var clientName = fmt.Sprintf("go-sdk-%s", uid)

func findDatabase(databases []Database, name string) *Database {
	for _, database := range databases {
		if database.Name == name {
			return &database
		}
	}
	return nil
}

func findEDB(edbs []EDB, name string) *EDB {
	for _, edb := range edbs {
		if edb.Name == name {
			return &edb
		}
	}
	return nil
}

func findModel(models []Model, name string) *Model {
	for _, model := range models {
		if model.Name == name {
			return &model
		}
	}
	return nil
}

// Test database management APIs.
func TestDatabase(t *testing.T) {
	client, err := NewTestClient()
	assert.Nil(t, err)
	defer TearDownEngine(client, engineName)
	defer TearDownDatabase(client, databaseName)

	EnsureEngine(t, client, engineName)
	EnsureDatabase(t, client, databaseName)

	if err := client.DeleteDatabase(databaseName); err != nil {
		assert.True(t, isErrNotFound(err))
	}

	database, err := client.CreateDatabase(databaseName)
	assert.Nil(t, err)
	assert.Equal(t, databaseName, database.Name)
	assert.Equal(t, "CREATED", database.State)

	database, err = client.GetDatabase(databaseName)
	assert.Nil(t, err)
	assert.Equal(t, databaseName, database.Name)
	assert.Equal(t, "CREATED", database.State)

	databases, err := client.ListDatabases()
	assert.Nil(t, err)
	database = findDatabase(databases, databaseName)
	assert.NotNil(t, database)
	assert.Equal(t, databaseName, database.Name)
	assert.Equal(t, "CREATED", database.State)

	databases, err = client.ListDatabases("state", "CREATED")
	assert.Nil(t, err)
	database = findDatabase(databases, databaseName)
	assert.NotNil(t, database)
	assert.Equal(t, databaseName, database.Name)
	assert.Equal(t, "CREATED", database.State)

	databases, err = client.ListDatabases("state", "NONSENSE")
	assert.Nil(t, err)
	database = findDatabase(databases, databaseName)
	assert.Nil(t, database)

	// missing filter value
	databases, err = client.ListDatabases("state")
	assert.Equal(t, ErrMissingFilterValue, err)

	edbs, err := client.ListEDBs(databaseName, engineName)
	assert.Nil(t, err)
	edb := findEDB(edbs, "rel")
	assert.NotNil(t, edb)

	modelNames, err := client.ListModelNames(databaseName, engineName)
	assert.Nil(t, err)
	assert.True(t, len(modelNames) > 0)
	assert.True(t, contains(modelNames, "rel/stdlib"))

	models, err := client.ListModels(databaseName, engineName)
	assert.Nil(t, err)
	assert.True(t, len(models) > 0)
	model := findModel(models, "rel/stdlib")
	assert.NotNil(t, model)
	assert.True(t, len(model.Value) > 0)

	model, err = client.GetModel(databaseName, engineName, "rel/stdlib")
	assert.Nil(t, err)
	assert.NotNil(t, model)
	assert.True(t, len(model.Value) > 0)

	err = client.DeleteDatabase(databaseName)
	assert.Nil(t, err)

	_, err = client.GetDatabase(databaseName)
	assert.True(t, isErrNotFound(err))

	databases, err = client.ListDatabases()
	assert.Nil(t, err)
	database = findDatabase(databases, databaseName)
	assert.Nil(t, err)
}

func findEngine(engines []Engine, name string) *Engine {
	for _, engine := range engines {
		if engine.State == "PROVISIONED" && engine.Name == name {
			return &engine
		}
	}
	return nil
}

// Test engine management APIs.
func TestEngine(t *testing.T) {
	client, err := NewTestClient()
	assert.Nil(t, err)
	defer TearDownEngine(client, engineName)
	defer TearDownDatabase(client, databaseName)

	if err := client.DeleteEngine(engineName); err != nil {
		assert.True(t, isErrNotFound(err))
	}

	engine, err := client.CreateEngine(engineName, "XS")
	assert.Nil(t, err)
	assert.Equal(t, engineName, engine.Name)
	assert.Equal(t, "PROVISIONED", engine.State)

	engine, err = client.GetEngine(engineName)
	assert.Nil(t, err)
	assert.Equal(t, engineName, engine.Name)
	assert.Equal(t, "PROVISIONED", engine.State)
	assert.Equal(t, "XS", engine.Size)

	engines, err := client.ListEngines()
	assert.Nil(t, err)
	engine = findEngine(engines, engineName)
	assert.NotNil(t, engine)
	assert.Equal(t, engineName, engine.Name)
	assert.Equal(t, "PROVISIONED", engine.State)
	assert.Equal(t, "XS", engine.Size)

	engines, err = client.ListEngines("state", "PROVISIONED")
	assert.Nil(t, err)
	engine = findEngine(engines, engineName)
	assert.NotNil(t, engine)
	assert.Equal(t, engineName, engine.Name)
	assert.Equal(t, "PROVISIONED", engine.State)
	assert.Equal(t, "XS", engine.Size)

	engines, err = client.ListEngines("state", "NONSENSE")
	assert.Nil(t, err)
	engine = findEngine(engines, engineName)
	assert.Nil(t, engine)

	err = client.DeleteEngine(engineName)
	assert.Nil(t, err)

	_, err = client.GetEngine(engineName)
	assert.True(t, isErrNotFound(err))

	engines, err = client.ListEngines()
	assert.Nil(t, err)
	engine = findEngine(engines, engineName)
	assert.Nil(t, engine)
}

// Test transaction execution.
func TestExecuteV1(t *testing.T) {
	client, err := NewTestClient()
	assert.Nil(t, err)
	defer TearDownEngine(client, engineName)
	defer TearDownDatabase(client, databaseName)

	EnsureEngine(t, client, engineName)
	EnsureDatabase(t, client, databaseName)

	query := "x, x^2, x^3, x^4 from x in {1; 2; 3; 4; 5}"

	rsp, err := client.ExecuteV1(databaseName, engineName, query, nil, true)
	assert.Nil(t, err)
	assert.Equal(t, false, rsp.Aborted)
	output := rsp.Output
	assert.Equal(t, 1, len(output))
	relation := output[0]
	relKey := relation.RelKey
	assert.Equal(t, "output", relKey.Name)
	assert.Equal(t, []string{"Int64", "Int64", "Int64"}, relKey.Keys)
	assert.Equal(t, []string{"Int64"}, relKey.Values)
	columns := relation.Columns
	expected := [][]interface{}{
		{1., 2., 3., 4., 5.},
		{1., 4., 9., 16., 25.},
		{1., 8., 27., 64., 125.},
		{1., 16., 81., 256., 625.}}
	assert.Equal(t, expected, columns)
}

// Test transaction asynchronous execution
func TestExecuteAsync(t *testing.T) {
	client, err := NewTestClient()
	assert.Nil(t, err)
	defer TearDownEngine(client, engineName)
	defer TearDownDatabase(client, databaseName)

	EnsureEngine(t, client, engineName)
	EnsureDatabase(t, client, databaseName)

	query := "x, x^2, x^3, x^4 from x in {1; 2; 3; 4; 5}"
	rsp, err := client.Execute(databaseName, engineName, query, nil, true)
	assert.Nil(t, err)

	//mock record
	pool := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "v1", Type: arrow.PrimitiveTypes.Int64},
			{Name: "v2", Type: arrow.PrimitiveTypes.Int64},
			{Name: "v3", Type: arrow.PrimitiveTypes.Int64},
			{Name: "v4", Type: arrow.PrimitiveTypes.Int64},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	b.Field(1).(*array.Int64Builder).AppendValues([]int64{1, 4, 9, 16, 25}, nil)
	b.Field(2).(*array.Int64Builder).AppendValues([]int64{1, 8, 27, 64, 125}, nil)
	b.Field(3).(*array.Int64Builder).AppendValues([]int64{1, 16, 81, 256, 625}, nil)

	rec := b.NewRecord()
	defer rec.Release()

	for i := 0; i < int(rsp.Results[0].Table.NumCols()); i++ {
		column := rsp.Results[0].Table.Column(i).(*array.Int64)
		expectedColumn := rec.Column(i).(*array.Int64)
		for j := 0; j < column.Len(); j++ {
			value := column.Value(j)
			expectedValue := expectedColumn.Value(j)
			assert.Equal(t, value, expectedValue)
		}
	}

	// mock metadata
	var metadata pb.MetadataInfo
	data, _ := os.ReadFile("./metadata.pb")
	proto.Unmarshal(data, &metadata)

	assert.Equal(t, rsp.Results[0].Metadata.Arguments, metadata.Relations[0].RelationId.Arguments)

	expectedProblems := []interface{}{}

	assert.Equal(t, rsp.Problems, expectedProblems)
}

func findRelation(relations []Relation, colName string) *Relation {
	for _, relation := range relations {
		keys := relation.RelKey.Keys
		if len(keys) == 0 {
			continue
		}
		name := keys[0]
		if name == colName {
			return &relation
		}
	}
	return nil
}

const sampleCSV = "" +
	"cocktail,quantity,price,date\n" +
	"\"martini\",2,12.50,\"2020-01-01\"\n" +
	"\"sazerac\",4,14.25,\"2020-02-02\"\n" +
	"\"cosmopolitan\",4,11.00,\"2020-03-03\"\n" +
	"\"bellini\",3,12.25,\"2020-04-04\"\n"

// Test loading CSV data using default options.
func TestLoadCSV(t *testing.T) {
	client, err := NewTestClient()
	assert.Nil(t, err)
	defer TearDownEngine(client, engineName)
	defer TearDownDatabase(client, databaseName)

	EnsureEngine(t, client, engineName)
	EnsureDatabase(t, client, databaseName)

	r := strings.NewReader(sampleCSV)
	rsp, err := client.LoadCSV(databaseName, engineName, "sample_csv", r, nil)
	assert.Nil(t, err)
	assert.Equal(t, false, rsp.Aborted)
	assert.Equal(t, 0, len(rsp.Output))
	assert.Equal(t, 0, len(rsp.Problems))

	rsp, err = client.ExecuteV1(databaseName, engineName, "def output = sample_csv", nil, true)
	assert.Equal(t, false, rsp.Aborted)
	assert.Equal(t, 4, len(rsp.Output))
	assert.Equal(t, 0, len(rsp.Problems))

	rel := findRelation(rsp.Output, ":date")
	assert.NotNil(t, rel)
	assert.Equal(t, 2, len(rel.Columns))
	assert.Equal(t, [][]interface{}{
		{2., 3., 4., 5.},
		{"2020-01-01", "2020-02-02", "2020-03-03", "2020-04-04"},
	}, rel.Columns)

	rel = findRelation(rsp.Output, ":price")
	assert.NotNil(t, rel)
	assert.Equal(t, 2, len(rel.Columns))
	assert.Equal(t, [][]interface{}{
		{2., 3., 4., 5.},
		{"12.50", "14.25", "11.00", "12.25"},
	}, rel.Columns)

	rel = findRelation(rsp.Output, ":quantity")
	assert.NotNil(t, rel)
	assert.Equal(t, 2, len(rel.Columns))
	assert.Equal(t, [][]interface{}{
		{2., 3., 4., 5.},
		{"2", "4", "4", "3"},
	}, rel.Columns)

	rel = findRelation(rsp.Output, ":cocktail")
	assert.NotNil(t, rel)
	assert.Equal(t, 2, len(rel.Columns))
	assert.Equal(t, [][]interface{}{
		{2., 3., 4., 5.},
		{"martini", "sazerac", "cosmopolitan", "bellini"},
	}, rel.Columns)
}

// Test loading CSV data with no header.
func TestLoadCSVNoHeader(t *testing.T) {
	client, err := NewTestClient()
	assert.Nil(t, err)
	defer TearDownEngine(client, engineName)
	defer TearDownDatabase(client, databaseName)

	EnsureEngine(t, client, engineName)
	EnsureDatabase(t, client, databaseName)

	const sampleNoHeader = "" +
		"\"martini\",2,12.50,\"2020-01-01\"\n" +
		"\"sazerac\",4,14.25,\"2020-02-02\"\n" +
		"\"cosmopolitan\",4,11.00,\"2020-03-03\"\n" +
		"\"bellini\",3,12.25,\"2020-04-04\"\n"

	r := strings.NewReader(sampleNoHeader)
	opts := NewCSVOptions().WithHeaderRow(0)
	rsp, err := client.LoadCSV(databaseName, engineName, "sample_no_header", r, opts)
	assert.Nil(t, err)
	assert.Equal(t, false, rsp.Aborted)
	assert.Equal(t, 0, len(rsp.Output))
	assert.Equal(t, 0, len(rsp.Problems))

	rsp, err = client.ExecuteV1(databaseName, engineName, "def output = sample_no_header", nil, true)
	assert.Equal(t, false, rsp.Aborted)
	assert.Equal(t, 4, len(rsp.Output))
	assert.Equal(t, 0, len(rsp.Problems))

	rel := findRelation(rsp.Output, ":COL1")
	assert.NotNil(t, rel)
	assert.Equal(t, 2, len(rel.Columns))
	assert.Equal(t, [][]interface{}{
		{1., 2., 3., 4.},
		{"martini", "sazerac", "cosmopolitan", "bellini"},
	}, rel.Columns)

	rel = findRelation(rsp.Output, ":COL2")
	assert.NotNil(t, rel)
	assert.Equal(t, 2, len(rel.Columns))
	assert.Equal(t, [][]interface{}{
		{1., 2., 3., 4.},
		{"2", "4", "4", "3"},
	}, rel.Columns)

	rel = findRelation(rsp.Output, ":COL3")
	assert.NotNil(t, rel)
	assert.Equal(t, 2, len(rel.Columns))
	assert.Equal(t, [][]interface{}{
		{1., 2., 3., 4.},
		{"12.50", "14.25", "11.00", "12.25"},
	}, rel.Columns)

	rel = findRelation(rsp.Output, ":COL4")
	assert.NotNil(t, rel)
	assert.Equal(t, 2, len(rel.Columns))
	assert.Equal(t, [][]interface{}{
		{1., 2., 3., 4.},
		{"2020-01-01", "2020-02-02", "2020-03-03", "2020-04-04"},
	}, rel.Columns)
}

// Test loading CSV data with alternate syntax options.
func TestLoadCSVAltSyntax(t *testing.T) {
	client, err := NewTestClient()
	assert.Nil(t, err)
	defer TearDownEngine(client, engineName)
	defer TearDownDatabase(client, databaseName)

	EnsureEngine(t, client, engineName)
	EnsureDatabase(t, client, databaseName)

	const sampleAltSyntax = "" +
		"cocktail|quantity|price|date\n" +
		"'martini'|2|12.50|'2020-01-01'\n" +
		"'sazerac'|4|14.25|'2020-02-02'\n" +
		"'cosmopolitan'|4|11.00|'2020-03-03'\n" +
		"'bellini'|3|12.25|'2020-04-04'\n"

	r := strings.NewReader(sampleAltSyntax)
	opts := NewCSVOptions().WithDelim('|').WithQuoteChar('\'')
	rsp, err := client.LoadCSV(databaseName, engineName, "sample_alt_syntax", r, opts)
	assert.Nil(t, err)
	assert.Equal(t, false, rsp.Aborted)
	assert.Equal(t, 0, len(rsp.Output))
	assert.Equal(t, 0, len(rsp.Problems))

	rsp, err = client.ExecuteV1(
		databaseName, engineName, "def output = sample_alt_syntax", nil, true)
	assert.Equal(t, false, rsp.Aborted)
	assert.Equal(t, 4, len(rsp.Output))
	assert.Equal(t, 0, len(rsp.Problems))

	rel := findRelation(rsp.Output, ":date")
	assert.NotNil(t, rel)
	assert.Equal(t, 2, len(rel.Columns))
	assert.Equal(t, [][]interface{}{
		{2., 3., 4., 5.},
		{"2020-01-01", "2020-02-02", "2020-03-03", "2020-04-04"},
	}, rel.Columns)

	rel = findRelation(rsp.Output, ":price")
	assert.NotNil(t, rel)
	assert.Equal(t, 2, len(rel.Columns))
	assert.Equal(t, [][]interface{}{
		{2., 3., 4., 5.},
		{"12.50", "14.25", "11.00", "12.25"},
	}, rel.Columns)

	rel = findRelation(rsp.Output, ":quantity")
	assert.NotNil(t, rel)
	assert.Equal(t, 2, len(rel.Columns))
	assert.Equal(t, [][]interface{}{
		{2., 3., 4., 5.},
		{"2", "4", "4", "3"},
	}, rel.Columns)

	rel = findRelation(rsp.Output, ":cocktail")
	assert.NotNil(t, rel)
	assert.Equal(t, 2, len(rel.Columns))
	assert.Equal(t, [][]interface{}{
		{2., 3., 4., 5.},
		{"martini", "sazerac", "cosmopolitan", "bellini"},
	}, rel.Columns)
}

// Test loading CSV data with a schema definition.
func TestLoadCSVWithSchema(t *testing.T) {
	client, err := NewTestClient()
	assert.Nil(t, err)
	defer TearDownEngine(client, engineName)
	defer TearDownDatabase(client, databaseName)

	EnsureEngine(t, client, engineName)
	EnsureDatabase(t, client, databaseName)

	schema := map[string]string{
		"cocktail": "string",
		"quantity": "int",
		"price":    "decimal(64,2)",
		"date":     "date"}
	r := strings.NewReader(sampleCSV)
	opts := NewCSVOptions().WithSchema(schema)
	rsp, err := client.LoadCSV(databaseName, engineName, "sample_with_schema", r, opts)
	assert.Nil(t, err)
	assert.Equal(t, false, rsp.Aborted)
	assert.Equal(t, 0, len(rsp.Output))
	assert.Equal(t, 0, len(rsp.Problems))

	rsp, err = client.ExecuteV1(databaseName, engineName, "def output = sample_with_schema", nil, true)
	assert.Equal(t, false, rsp.Aborted)
	assert.Equal(t, 4, len(rsp.Output))
	assert.Equal(t, 0, len(rsp.Problems))

	rel := findRelation(rsp.Output, ":date")
	assert.NotNil(t, rel)
	assert.Equal(t, 2, len(rel.Columns))
	assert.Equal(t, [][]interface{}{
		{2., 3., 4., 5.},
		{"2020-01-01", "2020-02-02", "2020-03-03", "2020-04-04"},
	}, rel.Columns)

	rel = findRelation(rsp.Output, ":price")
	assert.NotNil(t, rel)
	assert.Equal(t, 2, len(rel.Columns))
	assert.Equal(t, [][]interface{}{
		{2., 3., 4., 5.},
		{12.50, 14.25, 11.00, 12.25},
	}, rel.Columns)

	rel = findRelation(rsp.Output, ":quantity")
	assert.NotNil(t, rel)
	assert.Equal(t, 2, len(rel.Columns))
	assert.Equal(t, [][]interface{}{
		{2., 3., 4., 5.},
		{2., 4., 4., 3.},
	}, rel.Columns)

	rel = findRelation(rsp.Output, ":cocktail")
	assert.NotNil(t, rel)
	assert.Equal(t, 2, len(rel.Columns))
	assert.Equal(t, [][]interface{}{
		{2., 3., 4., 5.},
		{"martini", "sazerac", "cosmopolitan", "bellini"},
	}, rel.Columns)
}

// Test loading JSON data.
func TestLoadJSON(t *testing.T) {
	client, err := NewTestClient()
	assert.Nil(t, err)
	defer TearDownEngine(client, engineName)
	defer TearDownDatabase(client, databaseName)

	EnsureEngine(t, client, engineName)
	EnsureDatabase(t, client, databaseName)

	const sampleJSON = "{" +
		"\"name\":\"Amira\",\n" +
		"\"age\":32,\n" +
		"\"height\":null,\n" +
		"\"pets\":[\"dog\",\"rabbit\"]}"

	r := strings.NewReader(sampleJSON)
	rsp, err := client.LoadJSON(databaseName, engineName, "sample_json", r)
	assert.Nil(t, err)
	assert.Equal(t, false, rsp.Aborted)
	assert.Equal(t, 0, len(rsp.Output))
	assert.Equal(t, 0, len(rsp.Problems))

	rsp, err = client.ExecuteV1(
		databaseName, engineName, "def output = sample_json", nil, true)
	assert.Nil(t, err)
	assert.Equal(t, false, rsp.Aborted)
	assert.Equal(t, 4, len(rsp.Output))
	assert.Equal(t, 0, len(rsp.Problems))

	rel := findRelation(rsp.Output, ":name")
	assert.NotNil(t, rel)
	assert.Equal(t, 1, len(rel.Columns))
	assert.Equal(t, [][]interface{}{{"Amira"}}, rel.Columns)

	rel = findRelation(rsp.Output, ":age")
	assert.NotNil(t, rel)
	assert.Equal(t, 1, len(rel.Columns))
	assert.Equal(t, [][]interface{}{{32.}}, rel.Columns)

	rel = findRelation(rsp.Output, ":height")
	assert.NotNil(t, rel)
	assert.Equal(t, 1, len(rel.Columns))
	assert.Equal(t, [][]interface{}{{nil}}, rel.Columns)

	rel = findRelation(rsp.Output, ":pets")
	assert.NotNil(t, rel)
	assert.Equal(t, 2, len(rel.Columns))
	assert.Equal(t, [][]interface{}{{1., 2.}, {"dog", "rabbit"}}, rel.Columns)
}

// Test model APIs.
func TestModels(t *testing.T) {
	client, err := NewTestClient()
	assert.Nil(t, err)
	defer TearDownEngine(client, engineName)
	defer TearDownDatabase(client, databaseName)

	EnsureEngine(t, client, engineName)
	EnsureDatabase(t, client, databaseName)

	const testModel = "def R = \"hello\", \"world\""

	r := strings.NewReader(testModel)
	rsp, err := client.LoadModel(databaseName, engineName, "test_model", r)
	assert.Nil(t, err)
	assert.Equal(t, false, rsp.Aborted)
	assert.Equal(t, 0, len(rsp.Output))
	assert.Equal(t, 0, len(rsp.Problems))

	model, err := client.GetModel(databaseName, engineName, "test_model")
	assert.Nil(t, err)
	assert.Equal(t, "test_model", model.Name)

	modelNames, err := client.ListModelNames(databaseName, engineName)
	assert.Nil(t, err)
	assert.True(t, contains(modelNames, "test_model"))

	models, err := client.ListModels(databaseName, engineName)
	assert.Nil(t, err)
	model = findModel(models, "test_model")
	assert.NotNil(t, model)

	rsp, err = client.DeleteModel(databaseName, engineName, "test_model")
	assert.Equal(t, false, rsp.Aborted)
	assert.Equal(t, 0, len(rsp.Output))
	assert.Equal(t, 0, len(rsp.Problems))

	_, err = client.GetModel(databaseName, engineName, "test_model")
	assert.True(t, isErrNotFound(err))

	modelNames, err = client.ListModelNames(databaseName, engineName)
	assert.Nil(t, err)
	assert.False(t, contains(modelNames, "test_model"))

	models, err = client.ListModels(databaseName, engineName)
	assert.Nil(t, err)
	model = findModel(models, "test_model")
	assert.Nil(t, model)
}

func findOAuthClient(clients []OAuthClient, id string) *OAuthClient {
	for _, client := range clients {
		if client.ID == id {
			return &client
		}
	}
	return nil
}

// Test OAuth Client APIs.
func TestOAuthClient(t *testing.T) {
	client, err := NewTestClient()
	assert.Nil(t, err)
	defer TearDownEngine(client, engineName)
	defer TearDownDatabase(client, databaseName)

	rsp, err := client.FindOAuthClient(clientName)
	assert.Nil(t, err)
	if rsp != nil {
		_, err = client.DeleteOAuthClient(rsp.ID)
		assert.Nil(t, err)
	}

	rsp, err = client.FindOAuthClient(clientName)
	assert.Nil(t, err)
	assert.Nil(t, rsp)

	rspExtra, err := client.CreateOAuthClient(clientName, nil)
	assert.Nil(t, err)
	assert.Equal(t, clientName, rspExtra.Name)

	clientID := rspExtra.ID

	rsp, err = client.FindOAuthClient(clientName)
	assert.Nil(t, err)
	assert.NotNil(t, rsp)
	assert.Equal(t, clientID, rsp.ID)
	assert.Equal(t, clientName, rsp.Name)

	rspExtra, err = client.GetOAuthClient(clientID)
	assert.Nil(t, err)
	assert.NotNil(t, rsp)
	assert.Equal(t, clientID, rspExtra.ID)
	assert.Equal(t, clientName, rspExtra.Name)

	clients, err := client.ListOAuthClients()
	assert.Nil(t, err)
	item := findOAuthClient(clients, clientID)
	assert.NotNil(t, item)
	assert.Equal(t, clientID, item.ID)
	assert.Equal(t, clientName, item.Name)

	deleteRsp, err := client.DeleteOAuthClient(clientID)
	assert.Nil(t, err)
	assert.Equal(t, clientID, deleteRsp.ID)

	rspExtra, err = client.GetOAuthClient(clientID)
	assert.True(t, isErrNotFound(err))

	rsp, err = client.FindOAuthClient(clientName)
	assert.Nil(t, err)
	assert.Nil(t, rsp)
}

func findUser(users []User, id string) *User {
	for _, user := range users {
		if user.ID == id {
			return &user
		}
	}
	return nil
}

func TestUser(t *testing.T) {
	client, err := NewTestClient()
	assert.Nil(t, err)
	defer TearDownEngine(client, engineName)
	defer TearDownDatabase(client, databaseName)

	rsp, err := client.FindUser(userEmail)
	assert.Nil(t, err)
	if rsp != nil {
		_, err = client.DeleteUser(rsp.ID)
		assert.Nil(t, err)
	}

	rsp, err = client.FindUser(userEmail)
	assert.Nil(t, err)
	assert.Nil(t, rsp)

	rsp, err = client.CreateUser(userEmail, nil)
	assert.Equal(t, userEmail, rsp.Email)
	assert.Equal(t, "ACTIVE", rsp.Status)
	assert.Equal(t, []string{"user"}, rsp.Roles)

	var userID = rsp.ID

	rsp, err = client.FindUser(userEmail)
	assert.Nil(t, err)
	assert.NotNil(t, rsp)
	assert.Equal(t, userID, rsp.ID)
	assert.Equal(t, userEmail, rsp.Email)

	rsp, err = client.GetUser(userID)
	assert.Nil(t, err)
	assert.NotNil(t, rsp)
	assert.Equal(t, userID, rsp.ID)
	assert.Equal(t, userEmail, rsp.Email)

	users, err := client.ListUsers()
	assert.Nil(t, err)
	user := findUser(users, userID)
	assert.NotNil(t, user)
	assert.Equal(t, userID, user.ID)
	assert.Equal(t, userEmail, user.Email)

	rsp, err = client.DisableUser(userID)
	assert.Nil(t, err)
	assert.Equal(t, userID, user.ID)
	assert.Equal(t, "INACTIVE", rsp.Status)

	rsp, err = client.EnableUser(userID)
	assert.Nil(t, err)
	assert.Equal(t, userID, user.ID)
	assert.Equal(t, "ACTIVE", rsp.Status)

	req := UpdateUserRequest{Status: "INACTIVE"}
	rsp, err = client.UpdateUser(userID, req)
	assert.Nil(t, err)
	assert.Equal(t, userID, user.ID)
	assert.Equal(t, "INACTIVE", rsp.Status)

	req = UpdateUserRequest{Status: "ACTIVE"}
	rsp, err = client.UpdateUser(userID, req)
	assert.Nil(t, err)
	assert.Equal(t, userID, user.ID)
	assert.Equal(t, "ACTIVE", rsp.Status)

	req = UpdateUserRequest{Roles: []string{"admin", "user"}}
	rsp, err = client.UpdateUser(userID, req)
	assert.Nil(t, err)
	assert.Equal(t, userID, user.ID)
	assert.Equal(t, "ACTIVE", rsp.Status)
	assert.Equal(t, []string{"admin", "user"}, rsp.Roles)

	req = UpdateUserRequest{Status: "INACTIVE", Roles: []string{"user"}}
	rsp, err = client.UpdateUser(userID, req)
	assert.Nil(t, err)
	assert.Equal(t, userID, user.ID)
	assert.Equal(t, "INACTIVE", rsp.Status)
	assert.Equal(t, []string{"user"}, rsp.Roles)

	// Cleanup
	deleteRsp, err := client.DeleteUser(userID)
	assert.Nil(t, err)
	assert.Equal(t, userID, deleteRsp.ID)

	rsp, err = client.FindUser(userEmail)
	assert.Nil(t, err)
	assert.Nil(t, rsp)
}
