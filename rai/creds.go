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
	"io"
	"time"
)

// todo: make sure CreatedOn is persisted as epoch seconds
type AccessToken struct {
	Token     string    `json:"access_token"`
	Scope     string    `json:"scope"`
	ExpiresIn int       `json:"expires_in"` // token lifetime in seconds
	CreatedOn time.Time `json:"created_on"`
}

func (a *AccessToken) Load(r io.Reader) error {
	if err := json.NewDecoder(r).Decode(a); err != nil {
		return err
	}
	a.CreatedOn = time.Now()
	return nil
}

func (a *AccessToken) String() string {
	return a.Token
}

func (a *AccessToken) Duration() time.Duration {
	return time.Duration(a.ExpiresIn) * time.Second
}

func (a *AccessToken) IsExpired() bool {
	if a.Duration() < time.Since(a.CreatedOn) {
		return true
	}
	return false
}

type ClientCredentials struct {
	AccessToken          *AccessToken `json:"accessToken"`
	ClientId             string       `json:"clientId"`
	ClientSecret         string       `json:"-"`
	ClientCredentialsUrl string       `json:"clientCredentialsUrl"`
}
