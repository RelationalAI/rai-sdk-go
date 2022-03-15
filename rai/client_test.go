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
func ensureDatabase(t *testing.T, client *Client) error {
	var err error
	_, err = client.GetDatabase(databaseName)
	if err != nil {
		assert.Equal(t, ErrNotFound, err)
		_, err = client.CreateDatabase(databaseName, engineName, true)
	}
	return err
}

// Ensure that the test engine exists.
func ensureEngine(t *testing.T, client *Client) error {
	var err error
	_, err = client.GetEngine(engineName)
	if err != nil {
		assert.Equal(t, ErrNotFound, err)
		_, err = client.CreateEngine(engineName, "XS")
	}
	return err
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

func TestDatabase(t *testing.T) {
	client, err := NewDefaultClient()
	assert.Nil(t, err)

	err = ensureEngine(t, client)
	assert.Nil(t, err)

	if _, err := client.DeleteDatabase(databaseName); err != nil {
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

	deleteRsp, err := client.DeleteDatabase(databaseName)
	assert.Nil(t, err)
	assert.True(t, deleteRsp.Name == databaseName)

	_, err = client.GetDatabase(databaseName)
	assert.Equal(t, err, ErrNotFound)

	databases, err = client.ListDatabases()
	assert.Nil(t, err)
	database = findDatabase(databases, databaseName)
	assert.Nil(t, err)
}
