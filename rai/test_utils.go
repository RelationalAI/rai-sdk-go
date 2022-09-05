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
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func NewTestClient() (*Client, error) {
	configPath, _ := expandUser(DefaultConfigFile)
	if _, err := os.Stat(configPath); err == nil {
		return NewDefaultClient()
	}

	var cfg Config

	clientId := os.Getenv("CLIENT_ID")
	clientSecret := os.Getenv("CLIENT_SECRET")
	clientCredentialsUrl := os.Getenv("CLIENT_CREDENTIALS_URL")

	configFmt := `
		[default]
		host=azure.relationalai.com
		region=us-east
		port=443
		scheme=https
		client_id=%s
		client_secret=%s
		client_credentials_url=%s
	`
	configSrc := fmt.Sprintf(configFmt, clientId, clientSecret, clientCredentialsUrl)
	LoadConfigString(configSrc, "default", &cfg)
	opts := ClientOptions{Config: cfg}
	return NewClient(context.Background(), &opts), nil
}

// Answers if the given list contains the given value
func contains(items []string, value string) bool {
	for _, v := range items {
		if v == value {
			return true
		}
	}
	return false
}

func isErrNotFound(err error) bool {
	e, ok := err.(HTTPError)
	if !ok {
		return false
	}
	return e.StatusCode == http.StatusNotFound
}

// Ensure that the test database exists.
func EnsureDatabase(t *testing.T, client *Client, dbname string) {
	if _, err := client.GetDatabase(dbname); err != nil {
		assert.True(t, isErrNotFound(err))
		_, err := client.CreateDatabase(dbname)
		assert.Nil(t, err)
	}
}

// Ensure that the test engine exists.
func EnsureEngine(t *testing.T, client *Client, engine string) {
	if _, err := client.GetEngine(engine); err != nil {
		assert.True(t, isErrNotFound(err))
		_, err = client.CreateEngine(engine, "XS")
		assert.Nil(t, err)
	}
}

func TearDownUser(client *Client, userEmail string) {
	user, _ := client.FindUser(userEmail)
	if user != nil {
		client.DeleteUser(user.ID)
	}
}

func TearDownOAuthUser(client *Client, clientName string) {
	c, _ := client.FindOAuthClient(clientName)
	if c != nil {
		client.DeleteOAuthClient(c.ID)
	}
}
