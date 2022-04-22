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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow/go/v7/arrow/ipc"
	"github.com/pkg/errors"
)

const userAgent = "raictl/" + Version

type ClientOptions struct {
	Config
	HTTPClient         *http.Client
	AccessTokenHandler AccessTokenHandler
}

func NewClientOptions(cfg *Config) *ClientOptions {
	return &ClientOptions{Config: *cfg}
}

type Client struct {
	ctx                context.Context
	Region             string
	Scheme             string
	Host               string
	Port               string
	http               *http.Client
	accessToken        string
	accessTokenHandler AccessTokenHandler
}

const DefaultHost = "azure.relationalai.com"
const DefaultPort = "443"
const DefaultRegion = "us-east"
const DefaultScheme = "https"

func NewClient(ctx context.Context, opts *ClientOptions) *Client {
	if opts == nil {
		opts = &ClientOptions{}
	}
	host := opts.Host
	if host == "" {
		host = DefaultHost
	}
	port := opts.Port
	if port == "" {
		port = DefaultPort
	}
	region := opts.Region
	if region == "" {
		region = DefaultRegion
	}
	scheme := opts.Scheme
	if scheme == "" {
		scheme = DefaultScheme
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{}
	}
	client := &Client{
		ctx:    ctx,
		Region: region,
		Scheme: scheme,
		Host:   host,
		Port:   port,
		http:   opts.HTTPClient}
	if opts.AccessTokenHandler != nil {
		client.accessTokenHandler = opts.AccessTokenHandler
	} else if opts.Credentials == nil {
		client.accessTokenHandler = NewNopAccessTokenHandler()
	} else {
		client.accessTokenHandler = NewClientCredentialsHandler(client, opts.Credentials)
	}
	return client
}

// Returns a new client using the background context and config settings from
// the named profile.
func NewClientFromConfig(profile string) (*Client, error) {
	var cfg Config

	if err := LoadConfigProfile(profile, &cfg); err != nil {
		return nil, err
	}
	opts := ClientOptions{Config: cfg}
	return NewClient(context.Background(), &opts), nil
}

// Returns a new client using the background context and config settings from
// the default profile.
func NewDefaultClient() (*Client, error) {
	return NewClientFromConfig(DefaultConfigProfile)
}

func (c *Client) Context() context.Context {
	return c.ctx
}

func (c *Client) SetContext(ctx context.Context) {
	c.ctx = ctx
}

func (c *Client) SetAccessTokenHandler(handler AccessTokenHandler) {
	c.accessTokenHandler = handler
}

// Ensures that the given path is a fully qualified URL.
func (c *Client) ensureUrl(path string) string {
	if len(path) > 0 && path[0] == '/' {
		return c.Url(path)
	}
	return path // assume its a URL
}

// Returns a URL constructed from given path.
func (c *Client) Url(path string) string {
	return fmt.Sprintf("%s://%s:%s%s", c.Scheme, c.Host, c.Port, path)
}

const getAccessTokenBody = `{
	"client_id": "%s",
	"client_secret": "%s",
	"audience": "%s",
	"grant_type": "client_credentials"
}`

// Returns the current access token
func (c *Client) AccessToken() (string, error) {
	return c.accessTokenHandler.GetAccessToken()
}

// Fetch a new access token using the given client credentials.
func (c *Client) GetAccessToken(creds *ClientCredentials) (*AccessToken, error) {
	audience := fmt.Sprintf("https://%s", c.Host)
	body := fmt.Sprintf(getAccessTokenBody, creds.ClientID, creds.ClientSecret, audience)
	req, err := http.NewRequest(http.MethodPost, creds.ClientCredentialsUrl, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req = req.WithContext(c.ctx)
	req = c.ensureHeaders(req)
	rsp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer rsp.Body.Close()
	token := &AccessToken{}
	if err = token.Load(rsp.Body); err != nil {
		return nil, err
	}
	return token, nil
}

// Authenticate the given request using the configured credentials.
func (c *Client) authenticate(req *http.Request) (*http.Request, error) {
	token, err := c.AccessToken()
	if err != nil || token == "" {
		return nil, err // don't authenticate the request
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	return req, nil
}

// Add any missing headers to the given request.
func (c *Client) ensureHeaders(req *http.Request) *http.Request {
	if v := req.Header.Get("accept"); v == "" {
		req.Header.Set("Accept", "application/json")
	}
	if v := req.Header.Get("content-type"); v == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if v := req.Header.Get("user-agent"); v == "" {
		req.Header.Set("User-Agent", userAgent)
	}
	return req
}

func (c *Client) newRequest(method, path string, args url.Values, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, c.ensureUrl(path), body)
	if err != nil {
		return nil, err
	}
	if len(args) > 0 {
		req.URL.RawQuery = args.Encode()
	}
	return req, nil
}

func (c *Client) Delete(path string, args url.Values, data, result interface{}) error {
	return c.request(http.MethodDelete, path, args, data, result)
}

func (c *Client) Get(path string, args url.Values, result interface{}) error {
	return c.request(http.MethodGet, path, args, nil, result)
}

func (c *Client) Patch(path string, args url.Values, data, result interface{}) error {
	return c.request(http.MethodPatch, path, args, data, result)
}

func (c *Client) Post(path string, args url.Values, data, result interface{}) error {
	return c.request(http.MethodPost, path, args, data, result)
}

func (c *Client) Put(path string, args url.Values, data, result interface{}) error {
	return c.request(http.MethodPut, path, args, data, result)
}

// Show the given value as JSON data.
func showJSON(v interface{}) {
	e := json.NewEncoder(os.Stdout)
	e.SetIndent("", "  ")
	e.Encode(v)
}

func showRequest(req *http.Request, data interface{}) {
	fmt.Printf("%s %s\n", req.Method, req.URL.String())
	for k := range req.Header {
		fmt.Printf("%s: %s\n", k, req.Header.Get(k))
	}
	if data != nil {
		showJSON(data)
	}
}

// Marshal the given item as a JSON string and return an io.Reader.
func marshal(item interface{}) (io.Reader, error) {
	if item == nil {
		return nil, nil
	}
	data, err := json.Marshal(item)
	if err != nil {
		return nil, err
	}
	return strings.NewReader(string(data)), nil
}

// parseArrowData parses arrow data
func parseArrowData(data []byte) ([]interface{}, error) {
	out := []interface{}{}
	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, nil
	}
	defer reader.Release()

	for reader.Next() {
		res := make(map[string]interface{})
		rec := reader.Record()
		for i := 0; i < int(rec.NumCols()); i++ {
			key := rec.ColumnName(i)
			value := rec.Column(i)
			res[key] = value
		}
		rec.Retain()
		out = append(out, res)
	}

	return out, nil
}

// parseMultipartResponse parses multipart response
func parseMultipartResponse(data []byte, boundary string) ([]byte, error) {
	output := []interface{}{}

	mr := multipart.NewReader(bytes.NewReader(data), boundary)
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		contentType := part.Header.Get("Content-Type")
		value, _ := ioutil.ReadAll(part)
		if contentType == "application/json" {
			var out map[string]interface{}
			err = json.Unmarshal(value, &out)
			if err != nil {
				return nil, err
			}
			output = append(output, out)
		} else if contentType == "application/vnd.apache.arrow.stream" {
			out, err := parseArrowData(value)
			if err != nil {
				return nil, err
			}
			output = append(output, out)
		} else {
			return nil, errors.Errorf("unsupported content-type: %s\n", contentType)
		}
	}

	outByte, err := json.Marshal(output)
	if err != nil {
		return nil, err
	}

	return outByte, nil
}

// Unmarshal the JSON object from the given response body.
func unmarshal(rsp *http.Response, result interface{}) error {
	data, err := ioutil.ReadAll(rsp.Body)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}

	mediaType, params, _ := mime.ParseMediaType(rsp.Header.Get("Content-Type"))
	boundary := params["boundary"]

	if mediaType == "application/json" {
		if err := json.Unmarshal(data, result); err != nil {
			return err
		}
	} else if mediaType == "multipart/form-data" {
		res, err := parseMultipartResponse(data, boundary)
		if err != nil {
			return err
		}

		if err := json.Unmarshal(res, result); err != nil {
			return err
		}
	}

	return nil
}

// Construct request, execute and unmarshal response.
func (c *Client) request(
	method, path string, args url.Values, data, result interface{},
) error {
	body, err := marshal(data)
	if err != nil {
		return err
	}
	req, err := c.newRequest(method, path, args, body)
	if err != nil {
		return err
	}
	req = c.ensureHeaders(req)
	req, err = c.authenticate(req)
	if err != nil {
		return err
	}
	// showRequest(req, data)
	rsp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer rsp.Body.Close()

	if result == nil {
		return nil
	}
	return unmarshal(rsp, &result)
}

type HTTPError struct {
	StatusCode int
	Body       string
}

func (e HTTPError) Error() string {
	statusText := http.StatusText(e.StatusCode)
	if e.Body != "" {
		return fmt.Sprintf("%d %s\n%s", e.StatusCode, statusText, e.Body)
	}
	return fmt.Sprintf("%d %s", e.StatusCode, statusText)
}

func newHTTPError(status int, body string) error {
	return HTTPError{StatusCode: status, Body: body}
}

var ErrNotFound = newHTTPError(http.StatusNotFound, "{\"status\":\"Not Found\",\"message\":\"compute not found\"}\n")

// Returns an HTTPError corresponding to the given response.
func httpError(rsp *http.Response) error {
	// assert rsp.Status < 200 || rsp.Status > 299
	data, err := ioutil.ReadAll(rsp.Body)
	if err != nil {
		data = []byte{}
	}
	return newHTTPError(rsp.StatusCode, string(data))
}

// Ansers if the given response has a status code representing an error.
func isErrorStatus(rsp *http.Response) bool {
	return rsp.StatusCode < 200 || rsp.StatusCode > 299
}

// Execute the given request and return the response or error.
func (c *Client) Do(req *http.Request) (*http.Response, error) {
	req = req.WithContext(c.ctx)
	rsp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if isErrorStatus(rsp) {
		defer rsp.Body.Close()
		return nil, httpError(rsp)
	}
	return rsp, nil
}

//
// RAI APIs
//

const (
	PathDatabase     = "/database"
	PathEngine       = "/compute"
	PathOAuthClients = "/oauth-clients"
	PathTransaction  = "/transaction"
	PathTransactions = "/transactions"
	PathUsers        = "/users"
)

func makePath(parts ...string) string {
	return strings.Join(parts, "/")
}

// Add the filter to the given query args.
func addFilter(args url.Values, name string, value interface{}) error {
	if value == nil {
		return nil // ignore
	}
	switch v := value.(type) {
	case int:
		args.Add(name, strconv.Itoa(v))
	case string:
		args.Add(name, v)
	case []string:
		for _, item := range v {
			args.Add(name, item)
		}
	default:
		return errors.Errorf("bad filter value '%v'", v)
	}
	return nil
}

// Add the contents of the filter map to the given query args.
func addFilterMap(args url.Values, m map[string]interface{}) error {
	for k, v := range m {
		if v == nil {
			continue // ignore
		}
		switch vv := v.(type) {
		case int:
			args.Add(k, strconv.Itoa(vv))
		case string:
			args.Add(k, vv)
		case []string:
			for _, item := range vv {
				if item == "" {
					continue // ignore
				}
				args.Add(k, item)
			}
		default:
			return errors.Errorf("bad filter value '%v'", vv)
		}
	}
	return nil
}

var ErrMissingFilterValue = errors.New("missing filter value")

// Construct a url.Values struct from the given filters.
func queryArgs(filters ...interface{}) (url.Values, error) {
	args := url.Values{}
	for i := 0; i < len(filters); i++ {
		filter := filters[i]
		switch item := filter.(type) {
		case map[string]interface{}:
			if err := addFilterMap(args, item); err != nil {
				return nil, err
			}
		case string:
			if i == len(filters)-1 {
				return nil, ErrMissingFilterValue
			}
			i++
			value := filters[i]
			if err := addFilter(args, item, value); err != nil {
				return nil, err
			}
		default:
			return nil, errors.Errorf("bad filter arg '%v'", item)
		}
	}
	return args, nil
}

//
// Databases
//

func (c *Client) CloneDatabase(
	database, engine, source string, overwrite bool,
) (*Database, error) {
	var result createDatabaseResponse
	tx := Transaction{
		Region:   c.Region,
		Database: database,
		Engine:   engine,
		Mode:     createMode(source, overwrite),
		Source:   source}
	data := tx.Payload()
	err := c.Post(PathTransaction, tx.QueryArgs(), data, &result)
	if err != nil {
		return nil, err
	}
	return c.GetDatabase(database)
}

func (c *Client) CreateDatabase(
	database, engine string, overwrite bool,
) (*Database, error) {
	var result createDatabaseResponse
	tx := Transaction{
		Region:   c.Region,
		Database: database,
		Engine:   engine,
		Mode:     createMode("", overwrite)}
	data := tx.Payload()
	err := c.Post(PathTransaction, tx.QueryArgs(), data, &result)
	if err != nil {
		return nil, err
	}
	return c.GetDatabase(database)
}

func (c *Client) DeleteDatabase(database string) error {
	var result deleteDatabaseResponse
	data := &DeleteDatabaseRequest{Name: database}
	return c.Delete(PathDatabase, nil, data, &result)
}

func (c *Client) GetDatabase(database string) (*Database, error) {
	args, err := queryArgs("name", database)
	if err != nil {
		return nil, err
	}
	var result getDatabaseResponse
	err = c.Get(PathDatabase, args, &result)
	if err != nil {
		return nil, err
	}
	if len(result.Databases) == 0 {
		return nil, ErrNotFound
	}
	return &result.Databases[0], nil
}

func (c *Client) ListDatabases(filters ...interface{}) ([]Database, error) {
	args, err := queryArgs(filters...)
	if err != nil {
		return nil, err
	}
	var result listDatabasesResponse
	err = c.Get(PathDatabase, args, &result)
	if err != nil {
		return nil, err
	}
	return result.Databases, nil
}

//
// Engines
//

// Answeres if the given state is a terminal state.
func isTerminalState(state, targetState string) bool {
	return state == targetState || strings.Contains(state, "FAILED")
}

// Request the creation of an engine, and wait for the opeartion to complete.
// This can block the caller for up to a minute.
func (c *Client) CreateEngine(engine, size string) (*Engine, error) {
	rsp, err := c.CreateEngineAsync(engine, size)
	if err != nil {
		return nil, err
	}
	for !isTerminalState(rsp.State, "PROVISIONED") {
		time.Sleep(5 * time.Second)
		if rsp, err = c.GetEngine(engine); err != nil {
			return nil, err
		}
	}
	return rsp, nil
}

// Request the creation of an engine, and immediately return. The process
// of provisioning a new engine can take up to a minute.
func (c *Client) CreateEngineAsync(engine, size string) (*Engine, error) {
	var result createEngineResponse
	data := &CreateEngineRequest{Region: c.Region, Name: engine, Size: size}
	err := c.Put(PathEngine, nil, data, &result)
	if err != nil {
		return nil, err
	}
	return &result.Engine, nil
}

// Request the deletion of an engine and wait for the operation to complete.
func (c *Client) DeleteEngine(engine string) error {
	rsp, err := c.DeleteEngineAsync(engine)
	if err != nil {
		return err
	}
	for !isTerminalState(rsp.State, "DELETED") {
		time.Sleep(3 * time.Second)
		if rsp, err = c.GetEngine(engine); err != nil {
			if err == ErrNotFound {
				return nil // successfully deleted
			}
			return err
		}
	}
	return nil
}

func (c *Client) DeleteEngineAsync(engine string) (*Engine, error) {
	var result deleteEngineResponse
	data := &DeleteEngineRequest{Name: engine}
	err := c.Delete(PathEngine, nil, data, &result)
	if err != nil {
		return nil, err
	}
	return c.GetEngine(engine) // normalize return type
}

func (c *Client) GetEngine(engine string) (*Engine, error) {
	args, err := queryArgs("name", engine, "deleted_on", "")
	if err != nil {
		return nil, err
	}
	var result getEngineResponse
	err = c.Get(PathEngine, args, &result)
	if err != nil {
		return nil, err
	}
	if len(result.Engines) == 0 {
		return nil, ErrNotFound
	}
	return &result.Engines[0], nil
}

func (c *Client) ListEngines(filters ...interface{}) ([]Engine, error) {
	args, err := queryArgs(filters...)
	if err != nil {
		return nil, err
	}
	var result listEnginesResponse
	err = c.Get(PathEngine, args, &result)
	if err != nil {
		return nil, err
	}
	return result.Engines, nil
}

//
// OAuth Clients
//

func (c *Client) CreateOAuthClient(
	name string, perms []string,
) (*OAuthClientExtra, error) {
	var result createOAuthClientResponse
	data := CreateOAuthClientRequest{Name: name, Permissions: perms}
	err := c.Post(PathOAuthClients, nil, data, &result)
	if err != nil {
		return nil, err
	}
	return &result.Client, nil
}

func (c *Client) DeleteOAuthClient(id string) (*DeleteOAuthClientResponse, error) {
	var result DeleteOAuthClientResponse
	err := c.Delete(makePath(PathOAuthClients, id), nil, nil, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// Returns the OAuth client with the given name or nil if it does not exist.
func (c *Client) FindOAuthClient(name string) (*OAuthClient, error) {
	clients, err := c.ListOAuthClients()
	if err != nil {
		return nil, err
	}
	for _, client := range clients {
		if client.Name == name {
			return &client, nil
		}
	}
	return nil, nil
}

func (c *Client) GetOAuthClient(id string) (*OAuthClientExtra, error) {
	var result getOAuthClientResponse
	err := c.Get(makePath(PathOAuthClients, id), nil, &result)
	if err != nil {
		return nil, err
	}
	return &result.Client, nil
}

func (c *Client) ListOAuthClients() ([]OAuthClient, error) {
	var result listOAuthClientsResponse
	err := c.Get(PathOAuthClients, nil, &result)
	if err != nil {
		return nil, err
	}
	return result.Clients, nil
}

//
// Models
//

func (c *Client) DeleteModel(
	database, engine, name string,
) (*TransactionResult, error) {
	return c.DeleteModels(database, engine, []string{name})
}

func (c *Client) DeleteModels(
	database, engine string, models []string,
) (*TransactionResult, error) {
	var result TransactionResult
	tx := Transaction{
		Region:   c.Region,
		Database: database,
		Engine:   engine,
		Mode:     "OPEN",
		Readonly: false}
	data := tx.Payload(makeDeleteModelsAction(models))
	err := c.Post(PathTransaction, tx.QueryArgs(), data, &result)
	if err != nil {
		return nil, err
	}
	return &result, err
}

func (c *Client) GetModel(database, engine, model string) (*Model, error) {
	var result listModelsResponse
	tx := NewTransaction(c.Region, database, engine, "OPEN")
	data := tx.Payload(makeListModelsAction())
	err := c.Post(PathTransaction, tx.QueryArgs(), data, &result)
	if err != nil {
		return nil, err
	}
	// assert len(result.Actions) == 1
	for _, item := range result.Actions[0].Result.Models {
		if item.Name == model {
			return &item, nil
		}
	}
	return nil, ErrNotFound
}

func (c *Client) LoadModel(
	database, engine, name string, r io.Reader,
) (*TransactionResult, error) {
	return c.LoadModels(database, engine, map[string]io.Reader{name: r})
}

func (c *Client) LoadModels(
	database, engine string, models map[string]io.Reader,
) (*TransactionResult, error) {
	var result TransactionResult
	tx := Transaction{
		Region:   c.Region,
		Database: database,
		Engine:   engine,
		Mode:     "OPEN",
		Readonly: false}
	actions := []DbAction{}
	for name, r := range models {
		model, err := ioutil.ReadAll(r)
		if err != nil {
			return nil, err
		}
		action := makeLoadModelAction(name, string(model))
		actions = append(actions, action)
	}
	data := tx.Payload(actions...)
	err := c.Post(PathTransaction, tx.QueryArgs(), data, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// Returns a list of model names for the given database.
func (c *Client) ListModelNames(database, engine string) ([]string, error) {
	var models listModelsResponse
	tx := NewTransaction(c.Region, database, engine, "OPEN")
	data := tx.Payload(makeListModelsAction())
	err := c.Post(PathTransaction, tx.QueryArgs(), data, &models)
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

// Returns the names of models installed in the given database.
func (c *Client) ListModels(database, engine string) ([]Model, error) {
	var models listModelsResponse
	tx := NewTransaction(c.Region, database, engine, "OPEN")
	data := tx.Payload(makeListModelsAction())
	err := c.Post(PathTransaction, tx.QueryArgs(), data, &models)
	if err != nil {
		return nil, err
	}
	actions := models.Actions
	// assert len(actions) == 1
	return actions[0].Result.Models, nil
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
	NoWaitDurable bool
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
		"nowait_durable": tx.NoWaitDurable,
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

// TransactionAsync is the envelope for an async transaction
type TransactionAsync struct {
	Database string
	Engine   string
	Source   string
	Readonly bool
}

func NewTransactionAsync(database, engine string) *TransactionAsync {
	return &TransactionAsync{
		Database: database,
		Engine:   engine}
}

func (tx *TransactionAsync) Payload(inputs map[string]string) map[string]interface{} {
	queryActionInputs := make([]interface{}, 0)
	for k, v := range inputs {
		queryActionInput, _ := makeQueryActionInput(k, v)
		queryActionInputs = append(queryActionInputs, queryActionInput)
	}

	data := map[string]interface{}{
		"dbname":      tx.Database,
		"readonly":    tx.Readonly,
		"engine_name": tx.Engine,
		"query":       tx.Source,
		"inputs":      queryActionInputs,
	}
	return data
}

func (tx *TransactionAsync) QueryArgs() url.Values {
	result := url.Values{}
	result.Add("dbname", tx.Database)
	result.Add("engine_name", tx.Engine)

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

func makeLoadModelAction(name, model string) DbAction {
	return DbAction{
		"type":    "InstallAction",
		"sources": []map[string]interface{}{makeQuerySource(name, model)}}
}

func makeListModelsAction() DbAction {
	return DbAction{"type": "ListSourceAction"}
}

func makeListEDBAction() DbAction {
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
		Region:   c.Region,
		Database: database,
		Engine:   engine,
		Mode:     "OPEN",
		Readonly: readonly,
	}
	queryAction, err := makeQueryAction(source, inputs)
	if err != nil {
		return nil, err
	}
	data := tx.Payload(queryAction)
	err = c.Post(PathTransaction, tx.QueryArgs(), data, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) ExecuteAsync(
	database, engine, source string,
	inputs map[string]string,
	readonly bool,
) (interface{}, error) {
	var result interface{}
	tx := TransactionAsync{
		Database: database,
		Engine:   engine,
		Source:   source,
		Readonly: readonly,
	}
	data := tx.Payload(inputs)
	err := c.Post(PathTransactions, tx.QueryArgs(), data, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) ExecuteAsyncWait(
	database, engine, source string,
	inputs map[string]string,
	readonly bool,
) (interface{}, error) {
	rsp, err := c.ExecuteAsync(database, engine, source, inputs, readonly)
	if err != nil {
		return nil, err
	}
	var id string
	mapRsp, ok := rsp.(map[string]interface{})
	if ok {
		id = mapRsp["id"].(string)
	} else {
		arrayRsp, _ := rsp.([]interface{})
		id = arrayRsp[0].(map[string]interface{})["id"].(string)
	}
	for {
		rsp, _ = c.GetTransaction(id)
		transaction := rsp.(map[string]interface{})["transaction"]
		state, _ := transaction.(map[string]interface{})["state"].(string)
		if state == "COMPLETED" || state == "ABORTED" {
			break
		}
		time.Sleep(2 * time.Second)
	}
	out := make(map[string]interface{})
	out["results"], _ = c.GetTransactionResults(id)
	out["metadata"], _ = c.GetTransactionMetadata(id)
	out["problems"], _ = c.GetTransactionProblems(id)
	return out, nil
}

func (c *Client) GetTransactions() (interface{}, error) {
	var result interface{}
	err := c.Get(makePath(PathTransactions), nil, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) GetTransaction(id string) (interface{}, error) {
	var result interface{}
	err := c.Get(makePath(PathTransactions, id), nil, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) GetTransactionResults(id string) (interface{}, error) {
	var result interface{}
	err := c.Get(makePath(PathTransactions, id, "results"), nil, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) GetTransactionMetadata(id string) (interface{}, error) {
	var result interface{}
	err := c.Get(makePath(PathTransactions, id, "metadata"), nil, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) GetTransactionProblems(id string) (interface{}, error) {
	var result interface{}
	err := c.Get(makePath(PathTransactions, id, "problems"), nil, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) ListEDBs(database, engine string) ([]EDB, error) {
	var result listEDBsResponse
	tx := &Transaction{
		Region:   c.Region,
		Database: database,
		Engine:   engine,
		Mode:     "OPEN",
		Readonly: true}
	data := tx.Payload(makeListEDBAction())
	err := c.Post(PathTransaction, tx.QueryArgs(), data, &result)
	if err != nil {
		return nil, err
	}
	if len(result.Actions) == 0 {
		return []EDB{}, nil
	}
	// assert len(result.Actions) == 1
	return result.Actions[0].Result.Rels, nil
}

// Note, the default LoadCSV value for HeaderRow is 1, so if you instantiate
// this using initializer syntax instead of `NewCSVOptions` make sure you set
// HeaderRow to the correct value, because the zero value means no header.
type CSVOptions struct {
	Schema     map[string]string
	HeaderRow  int
	Delim      rune
	EscapeChar rune
	QuoteChar  rune
}

func NewCSVOptions() *CSVOptions {
	return &CSVOptions{HeaderRow: 1}
}

func (opts *CSVOptions) WithDelim(delim rune) *CSVOptions {
	opts.Delim = delim
	return opts
}

func (opts *CSVOptions) WithEscapeChar(escapeChar rune) *CSVOptions {
	opts.EscapeChar = escapeChar
	return opts
}

func (opts *CSVOptions) WithQuoteChar(quoteChar rune) *CSVOptions {
	opts.QuoteChar = quoteChar
	return opts
}

func (opts *CSVOptions) WithHeaderRow(headerRow int) *CSVOptions {
	opts.HeaderRow = headerRow
	return opts
}

func (opts *CSVOptions) WithSchema(schema map[string]string) *CSVOptions {
	opts.Schema = schema
	return opts
}

// Generates Rel schema config defs for the given CSV options.
func genSchemaConfig(b *strings.Builder, opts *CSVOptions) {
	if opts == nil {
		return
	}
	schema := opts.Schema
	if schema == nil || len(schema) == 0 {
		return
	}
	count := 0
	b.WriteString("def config:schema = ")
	for k, v := range schema {
		if count > 0 {
			b.WriteRune(';')
		}
		b.WriteString(fmt.Sprintf("\n    :%s, \"%s\"", k, v))
		count++
	}
	b.WriteRune('\n')
}

func genLiteralInt(v int) string {
	return strconv.Itoa(v)
}

func genLiteralRune(v rune) string {
	if v == '\'' {
		return "'\\''"
	}
	return fmt.Sprintf("'%s'", string(v))
}

// Returns a Rel literal for the given value.
func genLiteral(v interface{}) string {
	switch vv := v.(type) {
	case int:
		return genLiteralInt(vv)
	case rune:
		return genLiteralRune(vv)
	}
	panic("unreached")
}

// Generates a Rel syntax config def for the given option name and value.
func genSyntaxOption(b *strings.Builder, name string, value interface{}) {
	lit := genLiteral(value)
	def := fmt.Sprintf("def config:syntax:%s = %s\n", name, lit)
	b.WriteString(def)
}

// Generates Rel syntax config defs for the given CSV options.
func genSyntaxConfig(b *strings.Builder, opts *CSVOptions) {
	if opts == nil {
		return
	}
	if opts.HeaderRow != 1 { // default: 1
		genSyntaxOption(b, "header_row", opts.HeaderRow)
	}
	if opts.Delim != 0 {
		genSyntaxOption(b, "delim", opts.Delim)
	}
	if opts.EscapeChar != 0 {
		genSyntaxOption(b, "escapechar", opts.EscapeChar)
	}
	if opts.QuoteChar != 0 {
		genSyntaxOption(b, "quotechar", opts.QuoteChar)
	}
}

// Generate Rel to load CSV data into a relation with the given name.
func genLoadCSV(relation string, opts *CSVOptions) string {
	b := new(strings.Builder)
	genSyntaxConfig(b, opts)
	genSchemaConfig(b, opts)
	b.WriteString("def config:data = data\n")
	b.WriteString(fmt.Sprintf("def insert:%s = load_csv[config]", relation))
	return b.String()
}

func (c *Client) LoadCSV(
	database, engine, relation string, r io.Reader, opts *CSVOptions,
) (*TransactionResult, error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	source := genLoadCSV(relation, opts)
	inputs := map[string]string{"data": string(data)}
	return c.Execute(database, engine, source, inputs, false)
}

func (c *Client) LoadJSON(
	database, engine, relation string, r io.Reader,
) (*TransactionResult, error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	b := new(strings.Builder)
	b.WriteString("def config:data = data\n")
	b.WriteString(fmt.Sprintf("def insert:%s = load_json[config]", relation))
	inputs := map[string]string{"data": string(data)}
	return c.Execute(database, engine, b.String(), inputs, false)
}

//
// Users
//

func (c *Client) CreateUser(email string, roles []string) (*User, error) {
	if len(roles) == 0 {
		roles = append(roles, "user")
	}
	var result createUserResponse
	data := &CreateUserRequest{Email: email, Roles: roles}
	err := c.Post(PathUsers, nil, data, &result)
	if err != nil {
		return nil, err
	}
	return &result.User, nil
}

func (c *Client) DeleteUser(id string) (*DeleteUserResponse, error) {
	var result DeleteUserResponse
	err := c.Delete(makePath(PathUsers, id), nil, nil, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) DisableUser(id string) (*User, error) {
	req := UpdateUserRequest{Status: "INACTIVE"}
	return c.UpdateUser(id, req)
}

func (c *Client) EnableUser(id string) (*User, error) {
	req := UpdateUserRequest{Status: "ACTIVE"}
	return c.UpdateUser(id, req)
}

// Returns the User with the given email or nil if it does not exist.
func (c *Client) FindUser(email string) (*User, error) {
	users, err := c.ListUsers()
	if err != nil {
		return nil, err
	}
	for _, user := range users {
		if user.Email == email {
			return &user, nil
		}
	}
	return nil, nil
}

func (c *Client) GetUser(id string) (*User, error) {
	var result getUserResponse
	err := c.Get(makePath(PathUsers, id), nil, &result)
	if err != nil {
		return nil, err
	}
	return &result.User, nil
}

func (c *Client) ListUsers() ([]User, error) {
	var result listUsersResponse
	err := c.Get(PathUsers, nil, &result)
	if err != nil {
		return nil, err
	}
	return result.Users, nil
}

func (c *Client) UpdateUser(id string, req UpdateUserRequest) (*User, error) {
	var result updateUserResponse
	err := c.Patch(makePath(PathUsers, id), nil, &req, &result)
	if err != nil {
		return nil, err
	}
	return &result.User, nil
}
