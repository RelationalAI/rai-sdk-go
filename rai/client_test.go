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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Answers if the given list contains the given value
func contains(items []string, value string) bool {
	for _, v := range items {
		if v == value {
			return true
		}
	}
	return false
}

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
	client := test.client

	if err := client.DeleteDatabase(test.databaseName); err != nil {
		assert.True(t, isErrNotFound(err))
	}

	database, err := client.CreateDatabase(test.databaseName)
	assert.Nil(t, err)
	assert.NotNil(t, database)
	if database != nil {
		assert.Equal(t, test.databaseName, database.Name)
		assert.Equal(t, "CREATED", database.State)
	}

	database, err = client.GetDatabase(test.databaseName)
	assert.Nil(t, err)
	assert.NotNil(t, database)
	if database != nil {
		assert.Equal(t, test.databaseName, database.Name)
		assert.Equal(t, "CREATED", database.State)
	}

	databases, err := client.ListDatabases()
	assert.Nil(t, err)
	database = findDatabase(databases, test.databaseName)
	assert.NotNil(t, database)
	if database != nil {
		assert.Equal(t, test.databaseName, database.Name)
		assert.Equal(t, "CREATED", database.State)
	}

	databases, err = client.ListDatabases("state", "CREATED")
	assert.Nil(t, err)
	database = findDatabase(databases, test.databaseName)
	assert.NotNil(t, database)
	if database != nil {
		assert.Equal(t, test.databaseName, database.Name)
		assert.Equal(t, "CREATED", database.State)
	}

	databases, err = client.ListDatabases("state", "NONSENSE")
	assert.Nil(t, err)
	database = findDatabase(databases, test.databaseName)
	assert.Nil(t, database)

	// missing filter value
	_, err = client.ListDatabases("state")
	assert.Equal(t, ErrMissingFilterValue, err)

	edbs, err := client.ListEDBs(test.databaseName, test.engineName)
	assert.Nil(t, err)
	edb := findEDB(edbs, "rel")
	assert.NotNil(t, edb)

	modelNames, err := client.ListModelNames(test.databaseName, test.engineName)
	assert.Nil(t, err)
	assert.True(t, len(modelNames) > 0)
	assert.True(t, contains(modelNames, "rel/stdlib"))

	models, err := client.ListModels(test.databaseName, test.engineName)
	assert.Nil(t, err)
	assert.True(t, len(models) > 0)
	model := findModel(models, "rel/stdlib")
	assert.NotNil(t, model)
	if model != nil {
		assert.True(t, len(model.Value) > 0)
	}

	model, err = client.GetModel(test.databaseName, test.engineName, "rel/stdlib")
	assert.Nil(t, err)
	assert.NotNil(t, model)
	if model != nil {
		assert.True(t, len(model.Value) > 0)
	}
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
	client := test.client

	engine, err := client.GetEngine(test.engineName)
	assert.Nil(t, err)
	assert.NotNil(t, engine)
	if engine != nil {
		assert.Equal(t, test.engineName, engine.Name)
		assert.Equal(t, "PROVISIONED", engine.State)
		assert.Equal(t, test.engineSize, engine.Size)
	}

	engines, err := client.ListEngines()
	assert.Nil(t, err)
	engine = findEngine(engines, test.engineName)
	assert.NotNil(t, engine)
	if engine != nil {
		assert.Equal(t, test.engineName, engine.Name)
		assert.Equal(t, "PROVISIONED", engine.State)
		assert.Equal(t, test.engineSize, engine.Size)
	}

	engines, err = client.ListEngines("state", "PROVISIONED")
	assert.Nil(t, err)
	engine = findEngine(engines, test.engineName)
	assert.NotNil(t, engine)
	if engine != nil {
		assert.Equal(t, test.engineName, engine.Name)
		assert.Equal(t, "PROVISIONED", engine.State)
		assert.Equal(t, test.engineSize, engine.Size)
	}

	engines, err = client.ListEngines("state", "NONSENSE")
	assert.Nil(t, err)
	engine = findEngine(engines, test.engineName)
	assert.Nil(t, engine)
}

// Test transaction execution.
func TestExecuteV1(t *testing.T) {
	client := test.client

	query := "x, x^2, x^3, x^4 from x in {1; 2; 3; 4; 5}"

	rsp, err := client.ExecuteV1(test.databaseName, test.engineName, query, nil, true)
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

func TestListTransactions(t *testing.T) {
	client := test.client

	query := "x, x^2, x^3, x^4 from x in {1; 2; 3; 4; 5}"
	txn, err := client.Execute(test.databaseName, test.engineName, query, nil, true)
	assert.Nil(t, err)

	expectedProblems := []Problem{}
	assert.Equal(t, expectedProblems, txn.Problems)

	txns, err := client.ListTransactions()
	assert.Nil(t, err)

	found := false
	for _, i := range txns {
		if i.ID == txn.Transaction.ID {
			found = true
			break
		}
	}
	assert.True(t, found, "transaction id not found in list")
}

// testing tag filters for transactions
func TestListTransactionsByTag(t *testing.T) {
	client := test.client

	query := "x, x^2, x^3, x^4 from x in {1; 2; 3; 4; 5}"
	tag := fmt.Sprintf("rai-sdk-go:%d", time.Now().Unix())
	txn, err := client.Execute(test.databaseName, test.engineName, query, nil, true, tag)
	assert.Nil(t, err)

	expectedProblems := []Problem{}
	assert.Equal(t, expectedProblems, txn.Problems)

	txns, err := client.ListTransactions(tag)
	assert.Nil(t, err)

	assert.Equal(t, 1, len(txns), "filter tag did not apply as expected")

}

func findRelation(relations []RelationV1, colName string) *RelationV1 {
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
	client := test.client

	r := strings.NewReader(sampleCSV)
	rsp, err := client.LoadCSV(test.databaseName, test.engineName, "sample_csv", r, nil)
	assert.Nil(t, err)
	assert.NotNil(t, rsp)
	if rsp != nil {
		assert.Equal(t, false, rsp.Aborted)
		assert.Equal(t, 0, len(rsp.Output))
		assert.Equal(t, 0, len(rsp.Problems))
	}

	rsp, err = client.ExecuteV1(test.databaseName, test.engineName, "def output = sample_csv", nil, true)
	assert.Nil(t, err)
	assert.NotNil(t, rsp)
	if rsp != nil {
		assert.Equal(t, false, rsp.Aborted)
		assert.Equal(t, 4, len(rsp.Output))
		assert.Equal(t, 0, len(rsp.Problems))
	}

	if rsp != nil {
		rel := findRelation(rsp.Output, ":date")
		assert.NotNil(t, rel)
		if rel != nil {
			assert.Equal(t, 2, len(rel.Columns))
			assert.Equal(t, [][]interface{}{
				{2., 3., 4., 5.},
				{"2020-01-01", "2020-02-02", "2020-03-03", "2020-04-04"},
			}, rel.Columns)
		}

		rel = findRelation(rsp.Output, ":price")
		assert.NotNil(t, rel)
		if rel != nil {
			assert.Equal(t, 2, len(rel.Columns))
			assert.Equal(t, [][]interface{}{
				{2., 3., 4., 5.},
				{"12.50", "14.25", "11.00", "12.25"},
			}, rel.Columns)
		}

		rel = findRelation(rsp.Output, ":quantity")
		assert.NotNil(t, rel)
		if rel != nil {
			assert.Equal(t, 2, len(rel.Columns))
			assert.Equal(t, [][]interface{}{
				{2., 3., 4., 5.},
				{"2", "4", "4", "3"},
			}, rel.Columns)

		}

		rel = findRelation(rsp.Output, ":cocktail")
		assert.NotNil(t, rel)
		if rel != nil {
			assert.Equal(t, 2, len(rel.Columns))
			assert.Equal(t, [][]interface{}{
				{2., 3., 4., 5.},
				{"martini", "sazerac", "cosmopolitan", "bellini"},
			}, rel.Columns)
		}
	}
}

// Test loading CSV data with no header.
func TestLoadCSVNoHeader(t *testing.T) {
	client := test.client

	const sampleNoHeader = "" +
		"\"martini\",2,12.50,\"2020-01-01\"\n" +
		"\"sazerac\",4,14.25,\"2020-02-02\"\n" +
		"\"cosmopolitan\",4,11.00,\"2020-03-03\"\n" +
		"\"bellini\",3,12.25,\"2020-04-04\"\n"

	r := strings.NewReader(sampleNoHeader)
	opts := NewCSVOptions().WithHeaderRow(0)
	rsp, err := client.LoadCSV(test.databaseName, test.engineName, "sample_no_header", r, opts)
	assert.Nil(t, err)
	assert.NotNil(t, rsp)
	if rsp != nil {
		assert.Equal(t, false, rsp.Aborted)
		assert.Equal(t, 0, len(rsp.Output))
		assert.Equal(t, 0, len(rsp.Problems))
	}

	rsp, err = client.ExecuteV1(test.databaseName, test.engineName, "def output = sample_no_header", nil, true)
	assert.Nil(t, err)
	assert.NotNil(t, rsp)
	if rsp != nil {
		assert.Equal(t, false, rsp.Aborted)
		assert.Equal(t, 4, len(rsp.Output))
		assert.Equal(t, 0, len(rsp.Problems))
	}

	rel := findRelation(rsp.Output, ":COL1")
	assert.NotNil(t, rel)
	if rel != nil {
		assert.Equal(t, 2, len(rel.Columns))
		assert.Equal(t, [][]interface{}{
			{1., 2., 3., 4.},
			{"martini", "sazerac", "cosmopolitan", "bellini"},
		}, rel.Columns)
	}

	rel = findRelation(rsp.Output, ":COL2")
	assert.NotNil(t, rel)
	if rel != nil {
		assert.Equal(t, 2, len(rel.Columns))
		assert.Equal(t, [][]interface{}{
			{1., 2., 3., 4.},
			{"2", "4", "4", "3"},
		}, rel.Columns)
	}

	rel = findRelation(rsp.Output, ":COL3")
	assert.NotNil(t, rel)
	if rel != nil {
		assert.Equal(t, 2, len(rel.Columns))
		assert.Equal(t, [][]interface{}{
			{1., 2., 3., 4.},
			{"12.50", "14.25", "11.00", "12.25"},
		}, rel.Columns)
	}

	rel = findRelation(rsp.Output, ":COL4")
	assert.NotNil(t, rel)
	if rel != nil {
		assert.Equal(t, 2, len(rel.Columns))
		assert.Equal(t, [][]interface{}{
			{1., 2., 3., 4.},
			{"2020-01-01", "2020-02-02", "2020-03-03", "2020-04-04"},
		}, rel.Columns)
	}
}

// Test loading CSV data with alternate syntax options.
func TestLoadCSVAltSyntax(t *testing.T) {
	client := test.client

	const sampleAltSyntax = "" +
		"cocktail|quantity|price|date\n" +
		"'martini'|2|12.50|'2020-01-01'\n" +
		"'sazerac'|4|14.25|'2020-02-02'\n" +
		"'cosmopolitan'|4|11.00|'2020-03-03'\n" +
		"'bellini'|3|12.25|'2020-04-04'\n"

	r := strings.NewReader(sampleAltSyntax)
	opts := NewCSVOptions().WithDelim('|').WithQuoteChar('\'')
	rsp, err := client.LoadCSV(test.databaseName, test.engineName, "sample_alt_syntax", r, opts)
	assert.Nil(t, err)
	assert.NotNil(t, rsp)
	if rsp != nil {
		assert.Equal(t, false, rsp.Aborted)
		assert.Equal(t, 0, len(rsp.Output))
		assert.Equal(t, 0, len(rsp.Problems))
	}

	rsp, err = client.ExecuteV1(
		test.databaseName, test.engineName, "def output = sample_alt_syntax", nil, true)
	assert.Nil(t, err)
	assert.NotNil(t, rsp)
	if rsp != nil {
		assert.Equal(t, false, rsp.Aborted)
		assert.Equal(t, 4, len(rsp.Output))
		assert.Equal(t, 0, len(rsp.Problems))
	}

	rel := findRelation(rsp.Output, ":date")
	assert.NotNil(t, rel)
	if rel != nil {
		assert.Equal(t, 2, len(rel.Columns))
		assert.Equal(t, [][]interface{}{
			{2., 3., 4., 5.},
			{"2020-01-01", "2020-02-02", "2020-03-03", "2020-04-04"},
		}, rel.Columns)
	}

	rel = findRelation(rsp.Output, ":price")
	assert.NotNil(t, rel)
	if rel != nil {
		assert.Equal(t, 2, len(rel.Columns))
		assert.Equal(t, [][]interface{}{
			{2., 3., 4., 5.},
			{"12.50", "14.25", "11.00", "12.25"},
		}, rel.Columns)
	}

	rel = findRelation(rsp.Output, ":quantity")
	assert.NotNil(t, rel)
	if rel != nil {
		assert.Equal(t, 2, len(rel.Columns))
		assert.Equal(t, [][]interface{}{
			{2., 3., 4., 5.},
			{"2", "4", "4", "3"},
		}, rel.Columns)
	}

	rel = findRelation(rsp.Output, ":cocktail")
	assert.NotNil(t, rel)
	if rel != nil {
		assert.Equal(t, 2, len(rel.Columns))
		assert.Equal(t, [][]interface{}{
			{2., 3., 4., 5.},
			{"martini", "sazerac", "cosmopolitan", "bellini"},
		}, rel.Columns)
	}
}

// Test loading CSV data with a schema definition.
func TestLoadCSVWithSchema(t *testing.T) {
	client := test.client

	schema := map[string]string{
		"cocktail": "string",
		"quantity": "int",
		"price":    "decimal(64,2)",
		"date":     "date"}
	r := strings.NewReader(sampleCSV)
	opts := NewCSVOptions().WithSchema(schema)
	rsp, err := client.LoadCSV(test.databaseName, test.engineName, "sample_with_schema", r, opts)
	assert.Nil(t, err)
	assert.NotNil(t, rsp)
	if rsp != nil {
		assert.Equal(t, false, rsp.Aborted)
		assert.Equal(t, 0, len(rsp.Output))
		assert.Equal(t, 0, len(rsp.Problems))
	}

	rsp, err = client.ExecuteV1(test.databaseName, test.engineName, "def output = sample_with_schema", nil, true)
	assert.Nil(t, err)
	assert.NotNil(t, rsp)
	if rsp != nil {
		assert.Equal(t, false, rsp.Aborted)
		assert.Equal(t, 4, len(rsp.Output))
		assert.Equal(t, 0, len(rsp.Problems))
	}

	rel := findRelation(rsp.Output, ":date")
	assert.NotNil(t, rel)
	if rel != nil {
		assert.Equal(t, 2, len(rel.Columns))
		assert.Equal(t, [][]interface{}{
			{2., 3., 4., 5.},
			{"2020-01-01", "2020-02-02", "2020-03-03", "2020-04-04"},
		}, rel.Columns)
	}

	rel = findRelation(rsp.Output, ":price")
	assert.NotNil(t, rel)
	if rel != nil {
		assert.Equal(t, 2, len(rel.Columns))
		assert.Equal(t, [][]interface{}{
			{2., 3., 4., 5.},
			{12.50, 14.25, 11.00, 12.25},
		}, rel.Columns)
	}

	rel = findRelation(rsp.Output, ":quantity")
	assert.NotNil(t, rel)
	if rel != nil {
		assert.Equal(t, 2, len(rel.Columns))
		assert.Equal(t, [][]interface{}{
			{2., 3., 4., 5.},
			{2., 4., 4., 3.},
		}, rel.Columns)
	}

	rel = findRelation(rsp.Output, ":cocktail")
	assert.NotNil(t, rel)
	if rel != nil {
		assert.Equal(t, 2, len(rel.Columns))
		assert.Equal(t, [][]interface{}{
			{2., 3., 4., 5.},
			{"martini", "sazerac", "cosmopolitan", "bellini"},
		}, rel.Columns)
	}
}

// Test loading JSON data.
func TestLoadJSON(t *testing.T) {
	client := test.client

	const sampleJSON = "{" +
		"\"name\":\"Amira\",\n" +
		"\"age\":32,\n" +
		"\"height\":null,\n" +
		"\"pets\":[\"dog\",\"rabbit\"]}"

	r := strings.NewReader(sampleJSON)
	rsp, err := client.LoadJSON(test.databaseName, test.engineName, "sample_json", r)
	assert.Nil(t, err)
	assert.NotNil(t, rsp)
	if rsp != nil {
		assert.Equal(t, false, rsp.Aborted)
		assert.Equal(t, 0, len(rsp.Output))
		assert.Equal(t, 0, len(rsp.Problems))
	}

	rsp, err = client.ExecuteV1(
		test.databaseName, test.engineName, "def output = sample_json", nil, true)
	assert.Nil(t, err)
	assert.NotNil(t, rsp)
	if rsp != nil {
		assert.Equal(t, false, rsp.Aborted)
		assert.Equal(t, 4, len(rsp.Output))
		assert.Equal(t, 0, len(rsp.Problems))
	}

	rel := findRelation(rsp.Output, ":name")
	assert.NotNil(t, rel)
	if rel != nil {
		assert.Equal(t, 1, len(rel.Columns))
		assert.Equal(t, [][]interface{}{{"Amira"}}, rel.Columns)
	}

	rel = findRelation(rsp.Output, ":age")
	assert.NotNil(t, rel)
	if rel != nil {
		assert.Equal(t, 1, len(rel.Columns))
		assert.Equal(t, [][]interface{}{{32.}}, rel.Columns)
	}

	rel = findRelation(rsp.Output, ":height")
	assert.NotNil(t, rel)
	if rel != nil {
		assert.Equal(t, 1, len(rel.Columns))
		assert.Equal(t, [][]interface{}{{nil}}, rel.Columns)
	}

	rel = findRelation(rsp.Output, ":pets")
	assert.NotNil(t, rel)
	if rel != nil {
		assert.Equal(t, 2, len(rel.Columns))
		assert.Equal(t, [][]interface{}{{1., 2.}, {"dog", "rabbit"}}, rel.Columns)
	}
}

// Test model APIs.
func TestModels(t *testing.T) {
	client := test.client

	const testModel = "def R = \"hello\", \"world\""

	r := strings.NewReader(testModel)
	rsp, err := client.LoadModel(test.databaseName, test.engineName, "test_model", r)
	assert.Nil(t, err)
	assert.NotNil(t, rsp)
	if rsp != nil {
		assert.Equal(t, false, rsp.Aborted)
		assert.Equal(t, 0, len(rsp.Output))
		assert.Equal(t, 0, len(rsp.Problems))
	}

	model, err := client.GetModel(test.databaseName, test.engineName, "test_model")
	assert.Nil(t, err)
	assert.NotNil(t, model)
	if model != nil {
		assert.Equal(t, "test_model", model.Name)
	}

	modelNames, err := client.ListModelNames(test.databaseName, test.engineName)
	assert.Nil(t, err)
	assert.True(t, contains(modelNames, "test_model"))

	models, err := client.ListModels(test.databaseName, test.engineName)
	assert.Nil(t, err)
	model = findModel(models, "test_model")
	assert.NotNil(t, model)

	rsp, err = client.DeleteModel(test.databaseName, test.engineName, "test_model")
	assert.Nil(t, err)
	assert.NotNil(t, rsp)
	if rsp != nil {
		assert.Equal(t, false, rsp.Aborted)
		assert.Equal(t, 0, len(rsp.Output))
		assert.Equal(t, 0, len(rsp.Problems))
	}

	_, err = client.GetModel(test.databaseName, test.engineName, "test_model")
	assert.True(t, isErrNotFound(err))

	modelNames, err = client.ListModelNames(test.databaseName, test.engineName)
	assert.Nil(t, err)
	assert.False(t, contains(modelNames, "test_model"))

	models, err = client.ListModels(test.databaseName, test.engineName)
	assert.Nil(t, err)
	model = findModel(models, "test_model")
	assert.Nil(t, model)
}

// Test OAuth Client APIs.
func TestOAuthClient(t *testing.T) {
	client := test.client

	rsp, err := client.FindOAuthClient(test.oauthClient)
	if err != nil {
		assert.Equal(t, ErrNotFound, err)
	}
	if rsp != nil {
		_, err = client.DeleteOAuthClient(rsp.ID)
		assert.Nil(t, err)
	}

	rspExtra, err := client.CreateOAuthClient(test.oauthClient, nil)
	assert.Nil(t, err)
	assert.NotNil(t, rspExtra)
	if rspExtra != nil {
		assert.Equal(t, test.oauthClient, rspExtra.Name)
	}

	if rspExtra != nil {
		clientID := rspExtra.ID

		rspExtra, err = client.GetOAuthClient(clientID)
		assert.Nil(t, err)
		assert.NotNil(t, rspExtra)
		if rspExtra != nil {
			assert.NotNil(t, rspExtra)
			assert.Equal(t, clientID, rspExtra.ID)
			assert.Equal(t, test.oauthClient, rspExtra.Name)
		}

		deleteRsp, err := client.DeleteOAuthClient(clientID)
		assert.Nil(t, err)
		assert.NotNil(t, deleteRsp)
		if deleteRsp != nil {
			assert.Equal(t, clientID, deleteRsp.ID)
		}

		rspExtra, err = client.GetOAuthClient(clientID)
		assert.Nil(t, rspExtra)
		assert.True(t, isErrNotFound(err))
	}
}

// Test User APIs.
func TestUser(t *testing.T) {
	client := test.client

	rsp, err := client.FindUser(test.userEmail)
	assert.Nil(t, err)
	if rsp != nil {
		_, err = client.DeleteUser(rsp.ID)
		assert.Nil(t, err)
	}

	rsp, err = client.CreateUser(test.userEmail, nil)
	assert.Nil(t, err)
	assert.NotNil(t, rsp)
	if rsp != nil {
		assert.Equal(t, test.userEmail, rsp.Email)
		assert.Equal(t, "ACTIVE", rsp.Status)
		assert.Equal(t, []string{"user"}, rsp.Roles)
	}

	if rsp != nil {
		var userID = rsp.ID

		user, err := client.GetUser(userID)
		assert.Nil(t, err)
		assert.NotNil(t, user)
		if user != nil {
			assert.Equal(t, userID, user.ID)
			assert.Equal(t, test.userEmail, user.Email)
		}

		rsp, err = client.DisableUser(userID)
		assert.Nil(t, err)
		assert.NotNil(t, rsp)
		if rsp != nil {
			assert.Equal(t, userID, rsp.ID)
			assert.Equal(t, "INACTIVE", rsp.Status)
		}

		rsp, err = client.EnableUser(userID)
		assert.Nil(t, err)
		assert.NotNil(t, rsp)
		if rsp != nil {
			assert.Equal(t, userID, rsp.ID)
			assert.Equal(t, "ACTIVE", rsp.Status)
		}

		req := UpdateUserRequest{Status: "INACTIVE"}
		rsp, err = client.UpdateUser(userID, req)
		assert.Nil(t, err)
		assert.NotNil(t, rsp)
		if rsp != nil {
			assert.Equal(t, userID, rsp.ID)
			assert.Equal(t, "INACTIVE", rsp.Status)
		}

		req = UpdateUserRequest{Status: "ACTIVE"}
		rsp, err = client.UpdateUser(userID, req)
		assert.Nil(t, err)
		assert.NotNil(t, rsp)
		if rsp != nil {
			assert.Equal(t, userID, rsp.ID)
			assert.Equal(t, "ACTIVE", rsp.Status)
		}

		req = UpdateUserRequest{Roles: []string{"admin", "user"}}
		rsp, err = client.UpdateUser(userID, req)
		assert.Nil(t, err)
		assert.NotNil(t, rsp)
		if rsp != nil {
			assert.Equal(t, userID, rsp.ID)
			assert.Equal(t, "ACTIVE", rsp.Status)
			assert.Equal(t, []string{"admin", "user"}, rsp.Roles)
		}

		req = UpdateUserRequest{Status: "INACTIVE", Roles: []string{"user"}}
		rsp, err = client.UpdateUser(userID, req)
		assert.Nil(t, err)
		assert.NotNil(t, rsp)
		if rsp != nil {
			assert.Equal(t, userID, rsp.ID)
			assert.Equal(t, "INACTIVE", rsp.Status)
			assert.Equal(t, []string{"user"}, rsp.Roles)
		}

		deleteRsp, err := client.DeleteUser(userID)
		assert.Nil(t, err)
		assert.NotNil(t, deleteRsp)
		if deleteRsp != nil {
			assert.Equal(t, userID, deleteRsp.ID)
		}

		rsp, err = client.GetUser(userID)
		assert.Nil(t, rsp)
		assert.True(t, isErrNotFound(err))
	}
}
