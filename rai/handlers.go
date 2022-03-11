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
	"encoding/json"
	"os"
	"os/user"
	"path"
)

// Implementation of the nop and default access token handlers.

type AccessTokenHandler interface {
	GetAccessToken(creds *ClientCredentials) (*AccessToken, error)
}

// This handler always returns a nil token, which results in requests not being
// authenticated.
type NopAccessTokenHandler struct {
	client *Client
}

func NewNopAccessTokenHandler(c *Client) NopAccessTokenHandler {
	return NopAccessTokenHandler{client: c}
}

func (h NopAccessTokenHandler) GetAccessToken(_ *ClientCredentials) (*AccessToken, error) {
	return nil, nil
}

type DefaultAccessTokenHandler struct {
	client *Client
}

// This handler caches tokens in ~/.rai/tokens.json. It will attempt to load
// a token from the cache file and if it is not found or has expired, it will
// delegate to client.GetAccessToken to retrieve a new token and will save it
// in the cache file.
func NewDefaultAccessTokenHandler(c *Client) DefaultAccessTokenHandler {
	return DefaultAccessTokenHandler{client: c}
}

// Returns the name of the token cache file.
func cacheName() (string, error) {
	usr, err := user.Current()
	if err != nil {
		return "", err
	}
	return path.Join(usr.HomeDir, ".rai", "tokens.json"), nil
}

// Read the access token corresponding to the given ClientID from the local
// token cache, returns nil if the token does not exist.
func readAccessToken(creds *ClientCredentials) (*AccessToken, error) {
	cache, err := readTokenCache()
	if err != nil {
		return nil, err
	}
	if token, ok := cache[creds.ClientId]; ok {
		return token, nil
	}
	return nil, nil // doesn't exit
}

func readTokenCache() (map[string]*AccessToken, error) {
	fname, err := cacheName()
	if err != nil {
		return nil, err
	}
	f, err := os.Open(fname)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var cache map[string]*AccessToken
	err = json.NewDecoder(f).Decode(&cache)
	if err != nil {
		return nil, err
	}
	return cache, nil
}

// Write the given token to the local token cache.
func writeAccessToken(creds *ClientCredentials, token *AccessToken) {
	cache, err := readTokenCache()
	if err != nil {
		cache = map[string]*AccessToken{}
	}
	cache[creds.ClientId] = token
	writeTokenCache(cache)
}

func writeTokenCache(cache map[string]*AccessToken) {
	fname, err := cacheName()
	if err != nil {
		return
	}
	f, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return
	}
	json.NewEncoder(f).Encode(cache)
	f.Close()
}

func (h DefaultAccessTokenHandler) GetAccessToken(creds *ClientCredentials) (*AccessToken, error) {
	token, err := readAccessToken(creds)
	if err == nil && token != nil && !token.IsExpired() {
		return token, nil
	}
	token, err = h.client.GetAccessToken(creds)
	if err != nil {
		return nil, err
	}
	writeAccessToken(creds, token)
	return token, nil
}
