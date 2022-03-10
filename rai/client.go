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
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/relationalai/raicloud-services/pkg/logger"
)

const userAgent = "raictl/" + Version

type ClientOptions struct {
	Config
	HTTPClient *http.Client
}

func NewClientOptions(cfg *Config) *ClientOptions {
	return &ClientOptions{Config: *cfg}
}

type GetAccessTokenFunc = func(creds *ClientCredentials) (*AccessToken, error)

type Client struct {
	ctx                context.Context
	Region             string
	Scheme             string
	Host               string
	Port               string
	Credentials        interface{}
	http               *http.Client
	getAccessTokenFunc func(creds *ClientCredentials) (*AccessToken, error)
}

func NewClient(ctx context.Context, opts *ClientOptions) *Client {
	if opts.Region == "" {
		opts.Region = "us-east"
	}
	if opts.Scheme == "" {
		opts.Scheme = "https"
	}
	if opts.Port == "" {
		opts.Port = "443"
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{}
	}
	return &Client{
		ctx:         ctx,
		Region:      opts.Region,
		Scheme:      opts.Scheme,
		Host:        opts.Host,
		Port:        opts.Port,
		Credentials: opts.Credentials,
		http:        opts.HTTPClient}
}

func (c *Client) Context() context.Context {
	return c.ctx
}

func (c *Client) SetAccessTokenFunc(fn GetAccessTokenFunc) {
	c.getAccessTokenFunc = fn
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

// Retrieves a new access token using the configured client credentials.
func (c *Client) GetAccessToken(creds *ClientCredentials) (*AccessToken, error) {
	audience := fmt.Sprintf("https://%s", c.Host)
	body := fmt.Sprintf(getAccessTokenBody, creds.ClientId, creds.ClientSecret, audience)
	req, err := http.NewRequest(http.MethodPost, creds.ClientCredentialsUrl, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	log := logger.FromContext(c.ctx)
	log.Infof("fetching access token from '%s'", creds.ClientCredentialsUrl)
	req = req.WithContext(c.ctx)
	req = c.defaultHeaders(req)
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

// Returns the current access token if valid, otherwise requests a new one.
func (c *Client) getAccessToken(creds *ClientCredentials) (*AccessToken, error) {
	token := creds.AccessToken
	if token == nil || token.IsExpired() {
		fetch := c.getAccessTokenFunc // fetch token callback
		if fetch == nil {
			fetch = c.GetAccessToken
		}
		token, err := fetch(creds)
		if err != nil {
			return nil, err
		}
		creds.AccessToken = token
	}
	return creds.AccessToken, nil
}

// Authenticate the given request using the configured credentials.
func (c *Client) authenticate(req *http.Request) (*http.Request, error) {
	if c.Credentials == nil {
		return req, nil
	}
	switch creds := c.Credentials.(type) {
	case *ClientCredentials:
		token, err := c.getAccessToken(creds)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token.String()))
		return req, nil
	}
	return nil, errors.New("invalid credential type")
}

// Add any missing headers to the given request.
func (c *Client) defaultHeaders(req *http.Request) *http.Request {
	if v := req.Header.Get("accept"); v == "" {
		req.Header.Set("Accept", "application/json")
	}
	if v := req.Header.Get("content-type"); v == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if v := req.Header.Get("host"); v == "" {
		req.Header.Set("Host", c.Host) // todo: don't set host, leave to lib
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

// Unmarshal the JSON object from the given response body.
func unmarshal(rsp *http.Response, result interface{}) error {
	data, err := ioutil.ReadAll(rsp.Body)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	err = json.Unmarshal(data, result)
	if err != nil {
		return err
	}
	return nil
}

// Construct request, execute and unmarshal response.
func (c *Client) request(method, path string, args url.Values, data, result interface{}) error {
	body, err := marshal(data)
	if err != nil {
		return err
	}
	req, err := c.newRequest(method, path, args, body)
	if err != nil {
		return err
	}
	req = c.defaultHeaders(req)
	req, err = c.authenticate(req)
	if err != nil {
		return err
	}
	showRequest(req, data)
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
	StatusText string
	Body       string
}

func (e *HTTPError) Error() string {
	if e.Body != "" {
		return fmt.Sprintf("%d %s\n%s", e.StatusCode, e.StatusText, e.Body)
	}
	return fmt.Sprintf("%d %s", e.StatusCode, e.StatusText)
}

func newHTTPError(status int, body string) error {
	return &HTTPError{StatusCode: status, StatusText: http.StatusText(status)}
}

var ErrNotFound = newHTTPError(http.StatusNotFound, "")

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
	log := logger.FromContext(c.ctx)
	log.Infof("%s %s", req.Method, req.URL.String())
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
