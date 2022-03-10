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
	"net/url"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/relationalai/raicloud-services/raictl/pkg/rai/rest"
)

// todo: remove UpdateDatabase
// todo: add types for interface{} responses

const (
	PathDatabase     = "/database"
	PathEngine       = "/compute"
	PathOAuthClients = "/oauth-clients"
	PathTransaction  = "/transaction"
	PathUsers        = "/users"
)

type Filters map[string]interface{} // string, []string, int, bool

func makePath(parts ...string) string {
	return strings.Join(parts, "/")
}

// Construct a url.Values struct from the given filters.
func queryArgs(filters ...Filters) (url.Values, error) {
	args := url.Values{}
	for _, filter := range filters {
		for k, v := range filter {
			switch vv := v.(type) {
			case string:
				args.Add(k, vv)
			case []string:
				for _, item := range vv {
					args.Add(k, item)
				}
			case int:
				args.Add(k, strconv.Itoa(vv))
			default:
				return nil, errors.Errorf("bad filter value '%v'", vv)
			}
		}
	}
	return args, nil
}

type Client struct {
	rest *rest.Client
}

func NewClient(ctx context.Context, opts *rest.ClientOptions) *Client {
	return &Client{rest: rest.NewClient(ctx, opts)}
}

func (c *Client) Context() context.Context {
	return c.rest.Context()
}

func (c *Client) GetAccessToken(creds *rest.ClientCredentials) (*rest.AccessToken, error) {
	return c.rest.GetAccessToken(creds)
}

func (c *Client) SetAccessTokenFunc(fn rest.GetAccessTokenFunc) {
	c.rest.SetAccessTokenFunc(fn)
}

//
// Databases
//

func (c *Client) CreateDatabase(database, engine, source string, overwrite bool) (*CreateDatabaseResponse, error) {
	var result CreateDatabaseResponse
	tx := Transaction{
		Region:   c.rest.Region,
		Database: database,
		Engine:   engine,
		Mode:     createMode(source, overwrite),
		Source:   source}
	data := tx.Payload()
	err := c.rest.Post(PathTransaction, tx.QueryArgs(), data, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) DeleteDatabase(database string) (*DeleteDatabaseResponse, error) {
	var result DeleteDatabaseResponse
	data := &DeleteDatabaseRequest{Name: database}
	err := c.rest.Delete(PathDatabase, nil, data, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) GetDatabase(database string) (*Database, error) {
	args, err := queryArgs(Filters{"name": database})
	if err != nil {
		return nil, err
	}
	var result GetDatabaseResponse
	err = c.rest.Get(PathDatabase, args, &result)
	if err != nil {
		return nil, err
	}
	if len(result.Databases) == 0 {
		return nil, rest.ErrNotFound
	}
	return &result.Databases[0], nil
}

func (c *Client) ListDatabases(filters ...Filters) ([]Database, error) {
	args, err := queryArgs(filters...)
	if err != nil {
		return nil, err
	}
	var result ListDatabasesResponse
	err = c.rest.Get(PathDatabase, args, &result)
	if err != nil {
		return nil, err
	}
	return result.Databases, nil
}

func (c *Client) UpdateDatabase(database string, update *UpdateDatabaseRequest) (interface{}, error) {
	var result interface{}
	err := c.rest.Post(PathDatabase, nil, update, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

//
// Engines
//

func (c *Client) CreateEngine(engine, size string) (*Engine, error) {
	var result CreateEngineResponse
	data := &CreateEngineRequest{Region: c.rest.Region, Name: engine, Size: size}
	err := c.rest.Put(PathEngine, nil, data, &result)
	if err != nil {
		return nil, err
	}
	return &result.Engine, nil
}

func (c *Client) DeleteEngine(engine string) (*DeleteEngineStatus, error) {
	var result DeleteEngineResponse
	data := &DeleteEngineRequest{Name: engine}
	err := c.rest.Delete(PathEngine, nil, data, &result)
	if err != nil {
		return nil, err
	}
	return &result.Status, nil
}

func (c *Client) GetEngine(engine string) (*Engine, error) {
	args, err := queryArgs(Filters{"name": engine, "deleted_on": ""})
	if err != nil {
		return nil, err
	}
	var result GetEngineResponse
	err = c.rest.Get(PathEngine, args, &result)
	if err != nil {
		return nil, err
	}
	if len(result.Engines) == 0 {
		return nil, rest.ErrNotFound
	}
	return &result.Engines[0], nil
}

func (c *Client) ListEngines(filters ...Filters) ([]Engine, error) {
	args, err := queryArgs(filters...)
	if err != nil {
		return nil, err
	}
	var result ListEnginesResponse
	err = c.rest.Get(PathEngine, args, &result)
	if err != nil {
		return nil, err
	}
	return result.Engines, nil
}

//
// OAuth Clients
//

func (c *Client) CreateOAuthClient(name string, perms []string) (*OAuthClientExtra, error) {
	var result CreateOAuthClientResponse
	data := CreateOAuthClientRequest{Name: name, Permissions: perms}
	err := c.rest.Post(PathOAuthClients, nil, data, &result)
	if err != nil {
		return nil, err
	}
	return &result.Client, nil
}

func (c *Client) DeleteOAuthClient(id string) (*DeleteOAuthClientResponse, error) {
	var result DeleteOAuthClientResponse
	err := c.rest.Delete(makePath(PathOAuthClients, id), nil, nil, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) GetOAuthClient(id string) (*OAuthClientExtra, error) {
	var result GetOAuthClientResponse
	err := c.rest.Get(makePath(PathOAuthClients, id), nil, &result)
	if err != nil {
		return nil, err
	}
	return &result.Client, nil
}

func (c *Client) ListOAuthClients() ([]OAuthClient, error) {
	var result ListOAuthClientsResponse
	err := c.rest.Get(PathOAuthClients, nil, &result)
	if err != nil {
		return nil, err
	}
	return result.Clients, nil
}

//
// Models
//

// todo: rename DeleteModels
func (c *Client) DeleteModel(database, engine string, models []string) (interface{}, error) {
	var result interface{}
	tx := Transaction{
		Region:   c.rest.Region,
		Database: database,
		Engine:   engine,
		Mode:     "OPEN",
		Readonly: false}
	data := tx.Payload(makeDeleteModelsAction(models))
	err := c.rest.Post(PathTransaction, tx.QueryArgs(), data, result)
	if err == nil {
		return nil, err
	}
	return result, err
}

func (c *Client) GetModel(database, engine, model string) (string, error) {
	var result ListModelsResponse
	tx := NewTransaction(c.rest.Region, database, engine, "OPEN")
	data := tx.Payload(makeListModelsAction())
	err := c.rest.Post(PathTransaction, tx.QueryArgs(), data, &result)
	if err != nil {
		return "", err
	}
	// assert len(result.Actions) == 1
	for _, item := range result.Actions[0].Result.Models {
		if item.Name == model {
			return item.Value, nil
		}
	}
	return "", rest.ErrNotFound
}

func (c *Client) InstallModels(
	database, engine string, models map[string]string,
) (interface{}, error) {
	var result interface{}
	tx := Transaction{
		Region:   c.rest.Region,
		Database: database,
		Engine:   engine,
		Mode:     "OPEN",
		Readonly: false}
	actions := []DbAction{}
	for name, model := range models {
		actions = append(actions, makeInstallModelAction(name, model))
	}
	data := tx.Payload(actions...)
	err := c.rest.Post(PathTransaction, tx.QueryArgs(), data, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Returns a list of model names for the given database.
func (c *Client) ListModels(database, engine string) ([]string, error) {
	var models ListModelsResponse
	tx := NewTransaction(c.rest.Region, database, engine, "OPEN")
	data := tx.Payload(makeListModelsAction())
	err := c.rest.Post(PathTransaction, tx.QueryArgs(), data, &models)
	if err != nil {
		return nil, err
	}
	actions := models.Actions
	// assert len(actions) == 1
	result := []string{}
	for _, model := range actions[0].Result.Models {
		result = append(result, model.Name)
	}
	return result, nil
}

//
// Transactions
//

type DbAction map[string]interface{}

// The transaction "request" envelope
type Transaction struct {
	Region        string
	Database      string
	Engine        string
	Mode          string
	Source        string
	Abort         bool
	Readonly      bool
	NowaitDurable bool
	Version       int
}

func NewTransaction(region, database, engine, mode string) *Transaction {
	return &Transaction{
		Region:   region,
		Database: database,
		Engine:   engine,
		Mode:     mode}
}

// Constructs a transaction request payload.
func (tx *Transaction) Payload(actions ...DbAction) map[string]interface{} {
	data := map[string]interface{}{
		"type":           "Transaction",
		"abort":          tx.Abort,
		"actions":        makeActions(actions...),
		"dbname":         tx.Database,
		"nowait_durable": tx.NowaitDurable,
		"readonly":       tx.Readonly,
		"version":        tx.Version}
	if tx.Engine != "" {
		data["computeName"] = tx.Engine
	}
	if tx.Source != "" {
		data["source_dbname"] = tx.Source
	}
	if tx.Mode != "" {
		data["mode"] = tx.Mode
	} else {
		data["mode"] = "OPEN"
	}
	return data
}

func (tx *Transaction) QueryArgs() url.Values {
	result := url.Values{}
	result.Add("dbname", tx.Database)
	result.Add("compute_name", tx.Engine)
	result.Add("open_mode", tx.Mode)
	result.Add("region", tx.Region)
	if tx.Source != "" {
		result.Add("source_dbname", tx.Source)
	}
	return result
}

// Wrap each of the given actions in a LabeledAction.
func makeActions(actions ...DbAction) []DbAction {
	result := []DbAction{}
	for i, action := range actions {
		item := map[string]interface{}{
			"name":   fmt.Sprintf("action%d", i),
			"type":   "LabeledAction",
			"action": action}
		result = append(result, item)
	}
	return result
}

// Returns the database open_mode based on the given source and overwrite args.
func createMode(source string, overwrite bool) string {
	var mode string
	if source != "" {
		if overwrite {
			mode = "CLONE_OVERWRITE"
		} else {
			mode = "CLONE"
		}
	} else {
		if overwrite {
			mode = "CREATE_OVERWRITE"
		} else {
			mode = "CREATE"
		}
	}
	return mode
}

func makeRelKey(name, key string) map[string]interface{} {
	return map[string]interface{}{
		"type":   "RelKey",
		"name":   name,
		"keys":   []string{key},
		"values": []string{}}
}

func reltype(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return "RAI_VariableSizeStrings.VariableSizeString", nil
	default:
		return "", errors.Errorf("bad query input type: '%T'", v)
	}
}

func makeQuerySource(name, model string) map[string]interface{} {
	return map[string]interface{}{
		"type":  "Source",
		"name":  name,
		"path":  "",
		"value": model}
}

func makeDeleteModelsAction(models []string) DbAction {
	return DbAction{"type": "ModifyWorkspaceAction", "delete_source": models}
}

func makeInstallModelAction(name, model string) DbAction {
	return DbAction{
		"type":    "InstallAction",
		"sources": []map[string]interface{}{makeQuerySource(name, model)}}
}

func makeListModelsAction() DbAction {
	return DbAction{"type": "ListSourceAction"}
}

func makeListEdbAction() DbAction {
	return DbAction{"type": "ListEdbAction"}
}

func makeQueryAction(source string, inputs map[string]string) (DbAction, error) {
	actionInputs := []map[string]interface{}{}
	for k, v := range inputs {
		actionInput, err := makeQueryActionInput(k, v)
		if err != nil {
			return nil, err
		}
		actionInputs = append(actionInputs, actionInput)
	}
	result := map[string]interface{}{
		"type":    "QueryAction",
		"source":  makeQuerySource("query", source),
		"persist": []string{},
		"inputs":  actionInputs,
		"outputs": []string{}}
	return result, nil
}

func makeQueryActionInput(name, value string) (map[string]interface{}, error) {
	typename, err := reltype(value)
	if err != nil {
		return nil, err
	}
	result := map[string]interface{}{
		"type":    "Relation",
		"columns": [][]string{{value}},
		"rel_key": makeRelKey(name, typename)}
	return result, nil
}

// Execute the given query, with the given optional query inputs.
func (c *Client) Execute(
	database, engine, source string,
	inputs map[string]string,
	readonly bool,
) (*TransactionResult, error) {
	var result TransactionResult
	tx := Transaction{
		Region:   c.rest.Region,
		Database: database,
		Engine:   engine,
		Mode:     "OPEN",
		Readonly: readonly}
	queryAction, err := makeQueryAction(source, inputs)
	if err != nil {
		return nil, err
	}
	data := tx.Payload(queryAction)
	err = c.rest.Post(PathTransaction, tx.QueryArgs(), data, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) ListEdbs(database, engine string) ([]Edb, error) {
	var result ListEdbsResponse
	tx := &Transaction{
		Region:   c.rest.Region,
		Database: database,
		Engine:   engine,
		Mode:     "OPEN",
		Readonly: true}
	data := tx.Payload(makeListEdbAction())
	err := c.rest.Post(PathTransaction, tx.QueryArgs(), data, &result)
	if err != nil {
		return nil, err
	}
	if len(result.Actions) == 0 {
		return []Edb{}, nil
	}
	// assert len(result.Actions) == 1
	return result.Actions[0].Result.Rels, nil
}

type CSVOption struct {
	k, v string
}

func (o CSVOption) String() string {
	return fmt.Sprintf("def config:syntax:%s=%s", o.k, o.v)
}

// Returns a Rel string literal for the given string.
func stringLiteral(s string) string {
	s = strings.Replace(s, "'", "\\'", -1)
	return fmt.Sprintf("'%s'", s)
}

func CSVHeaderRow(n int) CSVOption {
	return CSVOption{k: "header_row", v: strconv.Itoa(n)}
}

func CSVDelim(s string) CSVOption {
	return CSVOption{k: "delim", v: stringLiteral(s)}
}

func CSVEscapeChar(s string) CSVOption {
	return CSVOption{k: "escapechar", v: stringLiteral(s)}
}

func CSVQuoteChar(s string) CSVOption {
	return CSVOption{k: "quotechar", v: stringLiteral(s)}
}

func (c *Client) LoadCSV(database, engine, relation, data string, opts ...CSVOption) (interface{}, error) {
	inputs := map[string]string{"data": data}
	b := new(strings.Builder)
	for _, opt := range opts {
		b.WriteString(fmt.Sprintf("%s\n", opt.String()))
	}
	b.WriteString("def config:data = data\n")
	b.WriteString(fmt.Sprintf("def insert:%s = load_csv[config]", relation))
	return c.Execute(database, engine, b.String(), inputs, false)
}

func (c *Client) LoadJSON(database, engine, relation, data string) (interface{}, error) {
	inputs := map[string]string{"data": data}
	b := new(strings.Builder)
	b.WriteString("def config:data = data\n")
	b.WriteString(fmt.Sprintf("def insert:%s = load_json[config]", relation))
	return c.Execute(database, engine, b.String(), inputs, false)
}

//
// Users
//

func (c *Client) CreateUser(email string, roles []string) (interface{}, error) {
	if len(roles) == 0 {
		roles = append(roles, "user")
	}
	var result interface{}
	data := &CreateUserRequest{Email: email, Roles: roles}
	err := c.rest.Post(PathUsers, nil, data, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) GetUser(id string) (*User, error) {
	var result GetUserResponse
	err := c.rest.Get(makePath(PathUsers, id), nil, &result)
	if err != nil {
		return nil, err
	}
	return &result.User, nil
}

func (c *Client) ListUsers() ([]User, error) {
	var result ListUsersResponse
	err := c.rest.Get(PathUsers, nil, &result)
	if err != nil {
		return nil, err
	}
	return result.Users, nil
}

func (c *Client) UpdateUser(id string, update *UpdateUserRequest) (*User, error) {
	var result UpdateUserResponse
	err := c.rest.Patch(makePath(PathUsers, id), nil, update, &result)
	if err != nil {
		return nil, err
	}
	return &result.User, nil
}
