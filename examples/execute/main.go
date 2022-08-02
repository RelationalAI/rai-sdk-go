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
	"io/ioutil"
	"log"
	"os"

	"github.com/jessevdk/go-flags"
	"github.com/relationalai/rai-sdk-go/rai"
)

type Options struct {
	Database string `short:"d" long:"database" required:"true" description:"database name"`
	Engine   string `short:"e" long:"engine" required:"true" descriptio:"engine name"`
	Code     string `short:"c" long:"code" description:"rel source code"`
	File     string `short:"f" long:"file" description:"rel source file"`
	Readonly bool   `long:"readonly" description:"readonly query (default: false)"`
	Profile  string `long:"profile" default:"default" description:"config profile"`
}

func getCode(opts *Options) (string, error) {
	if opts.Code != "" {
		return opts.Code, nil
	}
	bytes, err := ioutil.ReadFile(opts.File)
	if err != nil {
		return "", nil
	}
	return string(bytes), nil
}

func run(opts *Options) error {
	client, err := rai.NewClientFromConfig(opts.Profile)
	if err != nil {
		return err
	}
	source, err := getCode(opts)
	if err != nil {
		return err
	}
	rsp, err := client.Execute(opts.Database, opts.Engine, source, nil, opts.Readonly)
	if err != nil {
		return err
	}
	rai.Print(rsp, 4)
	return nil
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
