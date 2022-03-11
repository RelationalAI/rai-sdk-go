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
	"fmt"
	"os"

	"github.com/jessevdk/go-flags"
	"github.com/relationalai/rai-sdk-go/rai"
)

type Options struct {
	Database  string `short:"d" long:"database" required:"true" description:"database name"`
	Engine    string `short:"e" long:"engine" required:"true" description:"engine name"`
	Source    string `short:"s" long:"source" required:"true" description:"source database name"`
	Overwrite bool   `long:"overwrite" description:"overwrite existing database"`
	Profile   string `long:"profile" default:"default" description:"config profile"`
}

func run(opts *Options) error {
	client, err := rai.NewClientFromConfig(opts.Profile)
	if err != nil {
		return err
	}
	rsp, err := client.CloneDatabase(opts.Database, opts.Engine, opts.Source, opts.Overwrite)
	if err != nil {
		return err
	}
	rai.Print(rsp, 4)
	return nil
}

func main() {
	var opts Options
	if _, err := flags.ParseArgs(&opts, os.Args); err != nil {
		os.Exit(0)
	}
	if err := run(&opts); err != nil {
		fmt.Println(err)
	}
}
