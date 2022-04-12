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
	"time"
)

// REST API v1

//
// Resources
//

type Database struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	Region        string `json:"region"`
	AccountName   string `json:"account_name"`
	CreatedBy     string `json:"created_by"`
	DeletedOn     string `json:"deleted_on"`
	DeletedBy     string `json:"deleted_by,omitempty"`
	DefaultEngine string `json:"default_compute_name,omitempty"`
	State         string `json:"state"`
}

type EDB struct {
	Name   string   `json:"name"`
	Keys   []string `json:"keys"`
	Values []string `json:"values"`
}

type Engine struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Region      string `json:"region"`
	AccountName string `json:"account_name"`
	CreatedBy   string `json:"created_by"`
	CreatedOn   string `json:"created_on,omitempty"` // todo: required?
	DeletedOn   string `json:"deleted_on,omitempty"`
	Size        string `json:"size"`
	State       string `json:"state"`
}

type Model struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type OAuthClient struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	AccountName string    `json:"account_name"`
	CreatedBy   string    `json:"created_by"`
	CreatedOn   time.Time `json:"created_on"`
}

type OAuthClientExtra struct {
	OAuthClient
	Permissions []string `json:"permissions"`
	Secret      string   `json:"secret"`
}

type User struct {
	AccountName string   `json:"account_name"`
	Email       string   `json:"email"`
	ID          string   `json:"id"`
	IDProviers  []string `json:"id_providers"`
	Roles       []string `json:"roles"`
	Status      string   `json:"status"`
}

//
// Transaction results
//

type Problem struct {
	Type        string `json:"type"`
	ErrorCode   string `json:"error_code"`
	IsError     bool   `json:"is_error"`
	IsException bool   `json:"is_exception"`
	Message     string `json:"message"`
	Report      string `json:"report"`
}

type RelKey struct {
	Name   string   `json:"name"`
	Keys   []string `json:"keys"`
	Values []string `json:"values"`
}

type Relation struct {
	RelKey  RelKey          `json:"rel_key"`
	Columns [][]interface{} `json:"columns"`
}

type TransactionResult struct {
	Aborted  bool       `json:"aborted"`
	Output   []Relation `json:"output"`
	Problems []Problem  `json:"problems"`
}

//
// Request/response payloads
//

type createDatabaseResponse struct {
	Aborted  bool          `json:"aborted"`
	Actions  []interface{} `json:"actions"`
	Output   []interface{} `json:"output"`
	Problems []interface{} `json:"problems"`
	Version  int           `json:"version"` // todo: gone
}

type CreateEngineRequest struct {
	Name   string `json:"name"`
	Size   string `json:"size"`
	Region string `json:"region"` // todo: isnt region part of the context?
}

type createEngineResponse struct {
	Engine Engine `json:"compute"`
}

type CreateOAuthClientRequest struct {
	Name        string   `json:"name"`
	Permissions []string `json:"permissions"`
}

type createOAuthClientResponse struct {
	Client OAuthClientExtra `json:"client"`
}

type getOAuthClientResponse struct {
	createOAuthClientResponse
}

type CreateUserRequest struct {
	Email string   `json:"email"`
	Roles []string `json:"roles"`
}

type createUserResponse struct {
	User User `json:"user"`
}

type DeleteDatabaseRequest struct {
	Name string `json:"name"`
}

type deleteDatabaseResponse struct {
	Name    string `json:"name"`
	Message string `json:"message"`
}

type DeleteEngineRequest struct {
	Name string `json:"name"`
}

type deleteEngineResponse struct {
	Status DeleteEngineStatus `json:"status"`
}

type DeleteEngineStatus struct {
	Name    string `json:"name"`
	State   string `json:"state"`
	Message string `json:"message"`
}

type DeleteOAuthClientResponse struct {
	ID      string `json:"client_id"`
	Message string `json:"message"`
}

type getDatabaseResponse struct {
	Databases []Database `json:"databases"`
}

type getEngineResponse struct {
	Engines []Engine `json:"computes"`
}

type getUserResponse struct {
	User User `json:"user"`
}

type listDatabasesResponse struct {
	Databases []Database `json:"databases"`
}

type listEDBsResponse struct {
	Actions []struct {
		Result struct {
			Rels []EDB `json:"rels"`
		} `json:"result"`
	} `json:"actions"`
}

type listEnginesResponse struct {
	Engines []Engine `json:"computes"`
}

type listOAuthClientsResponse struct {
	Clients []OAuthClient `json:"clients"`
}

type listModelsResponse struct {
	Actions []struct {
		Result struct {
			Models []Model `json:"sources"`
		} `json:"result"`
	} `json:"actions"`
}

type DeleteUserResponse struct {
	ID      string `json:"user_id"`
	Message string `json:"message"`
}

type listUsersResponse struct {
	Users []User `json:"users"`
}

type UpdateUserRequest struct {
	Status string   `json:"status,omitempty"`
	Roles  []string `json:"roles,omitempty"`
}

type updateUserResponse struct {
	User User `json:"user"`
}
