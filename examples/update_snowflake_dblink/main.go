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

package main

import (
	"log"
	"os"

	"github.com/jessevdk/go-flags"
	"github.com/relationalai/rai-sdk-go/rai"
)

type Options struct {
	Profile           string `long:"profile" default:"default" description:"config profile"`
	Integration       string `long:"integration" required:"true" description:"RAI integration name"`
	Database          string `long:"database" required:"true" description:"Snowflake database name"`
	Schema            string `long:"schema" required:"true" description:"Snowflake schema name"`
	Role              string `long:"role" required:"true" description:"Snowflake role"`
	Warehouse         string `long:"warehouse" required:"true" description:"Snowflake warehouse"`
	SnowflakeUsername string `long:"snowflakeUsername" default:"default" description:"Snowflake username"`
	SnowflakePassword string `long:"snowflakePassword" default:"default" description:"Snowflake password"`
}

func run(opts *Options) error {
	client, err := rai.NewClientFromConfig(opts.Profile)
	if err != nil {
		return err
	}
	creds := rai.SnowflakeCredentials{Username: opts.SnowflakeUsername, Password: opts.SnowflakePassword}
	return client.UpdateSnowflakeDatabaseLink(opts.Integration, opts.Database, opts.Schema, opts.Role, opts.Warehouse, &creds)
}

func main() {
	var opts Options
	if _, err := flags.ParseArgs(&opts, os.Args); err != nil {
		os.Exit(1)
	}
	if err := run(&opts); err != nil {
		log.Fatal(err)
	}
}
