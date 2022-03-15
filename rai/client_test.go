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
	"testing"

	"github.com/stretchr/testify/assert"
)

const databaseName = "sdk-test"
const engineName = "sdk-test"

// Answers if the given list contains the given value
func contains(items []string, value string) bool {
	for _, v := range items {
		if v == value {
			return true
		}
	}
	return false
}

// Ensure that the test database exists.
func ensureDatabase(t *testing.T, client *Client) {
	ensureEngine(t, client)
	_, err := client.CreateDatabase(databaseName, engineName, true)
	assert.Nil(t, err)
}

// Ensure that the test engine exists.
func ensureEngine(t *testing.T, client *Client) {
	var err error
	_, err = client.GetEngine(engineName)
	if err != nil {
		assert.Equal(t, ErrNotFound, err)
		_, err = client.CreateEngine(engineName, "XS")
		assert.Nil(t, err)
	}
}

func findDatabase(databases []Database, name string) *Database {
	for _, database := range databases {
		if database.Name == name {
			return &database
		}
	}
	return nil
}

func findEdb(edbs []Edb, name string) *Edb {
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

// Test the database management APIs.
func TestDatabase(t *testing.T) {
	client, err := NewDefaultClient()
	assert.Nil(t, err)

	ensureEngine(t, client)

	if err := client.DeleteDatabase(databaseName); err != nil {
		assert.Equal(t, ErrNotFound, err)
	}

	database, err := client.CreateDatabase(databaseName, engineName, false)
	assert.Nil(t, err)
	assert.Equal(t, databaseName, database.Name)
	assert.Equal(t, "CREATED", database.State)

	database, err = client.CreateDatabase(databaseName, engineName, true) // overwrite
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

	edbs, err := client.ListEdbs(databaseName, engineName)
	assert.Nil(t, err)
	edb := findEdb(edbs, "rel")
	assert.NotNil(t, edb)

	modelNames, err := client.ListModelNames(databaseName, engineName)
	assert.Nil(t, err)
	assert.True(t, len(modelNames) > 0)
	assert.True(t, contains(modelNames, "stdlib"))

	models, err := client.ListModels(databaseName, engineName)
	assert.Nil(t, err)
	assert.True(t, len(models) > 0)
	model := findModel(models, "stdlib")
	assert.NotNil(t, model)
	assert.True(t, len(model.Value) > 0)

	model, err = client.GetModel(databaseName, engineName, "stdlib")
	assert.NotNil(t, model)
	assert.True(t, len(model.Value) > 0)

	err = client.DeleteDatabase(databaseName)
	assert.Nil(t, err)

	_, err = client.GetDatabase(databaseName)
	assert.Equal(t, err, ErrNotFound)

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

// Test the engine management APIs.
func TestEngine(t *testing.T) {
	client, err := NewDefaultClient()
	assert.Nil(t, err)

	if err := client.DeleteEngine(engineName); err != nil {
		assert.Equal(t, ErrNotFound, err)
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
	assert.Equal(t, ErrNotFound, err)

	engines, err = client.ListEngines()
	assert.Nil(t, err)
	engine = findEngine(engines, engineName)
	assert.Nil(t, engine)
}

// Test transaction execution.
func TestExecute(t *testing.T) {
	client, err := NewDefaultClient()
	assert.Nil(t, err)

	ensureDatabase(t, client)

	query := "x, x^2, x^3, x^4 from x in {1; 2; 3; 4; 5}"

	rsp, err := client.Execute(databaseName, engineName, query, nil, true)
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
