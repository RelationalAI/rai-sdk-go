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
	"os/user"
	"path"
	"strings"

	"github.com/pkg/errors"
	"gopkg.in/ini.v1"
)

const DefaultConfigFile = "~/.rai/config"
const DefaultConfigProfile = "default"

const defaultClientCredentialsUrl = "https://login.relationalai.com/oauth/token"

type Config struct {
	Region      string      `json:"region"`
	Scheme      string      `json:"scheme"`
	Host        string      `json:"host"`
	Port        string      `json:"port"`
	Credentials interface{} `json:"credentials"`
}

// Expand the given file path if it start with a ~/
func expandUser(fname string) (string, error) {
	if strings.HasPrefix(fname, "~/") {
		usr, err := user.Current()
		if err != nil {
			return "", err
		}
		return path.Join(usr.HomeDir, fname[2:]), nil
	}
	return fname, nil
}

// Load the named stanza from the named config file.
func loadStanza(fname, profile string) (*ini.Section, error) {
	info, err := ini.Load(fname)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading config file '%s'", fname)
	}
	stanza := info.Section(profile)
	if stanza == nil {
		return nil, errors.Errorf("config profile '%s' not found", profile)
	}
	return stanza, nil
}

// Load settings from the default profile of the default config file.
func LoadConfig(cfg *Config) error {
	return LoadConfigFile(DefaultConfigFile, DefaultConfigProfile, cfg)
}

// Load settings from the given profile in the default config file.
func LoadConfigProfile(profile string, cfg *Config) error {
	return LoadConfigFile(DefaultConfigFile, profile, cfg)
}

// Load settings from the given profile of the named config file.
func LoadConfigFile(fname, profile string, cfg *Config) error {
	fname, err := expandUser(fname)
	if err != nil {
		return err
	}
	stanza, err := loadStanza(fname, profile)
	if err != nil {
		return err
	}
	if v := stanza.Key("region").String(); v != "" {
		cfg.Region = v
	}
	if v := stanza.Key("scheme").String(); v != "" {
		cfg.Scheme = v
	}
	if v := stanza.Key("host").String(); v != "" {
		cfg.Host = v
	}
	if v := stanza.Key("port").String(); v != "" {
		cfg.Port = v
	}
	clientId := stanza.Key("client_id").String()
	clientSecret := stanza.Key("client_secret").String()
	if clientId != "" && clientSecret != "" {
		clientCredentialsUrl := defaultClientCredentialsUrl
		if v := stanza.Key("client_credentials_url").String(); v != "" {
			clientCredentialsUrl = v
		}
		cfg.Credentials = &ClientCredentials{
			ClientId:             clientId,
			ClientSecret:         clientSecret,
			ClientCredentialsUrl: clientCredentialsUrl}
	}
	return nil
}
