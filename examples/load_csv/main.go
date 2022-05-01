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
	"path/filepath"
	"strings"

	"github.com/jessevdk/go-flags"
	"github.com/relationalai/rai-sdk-go/rai"
)

type Options struct {
	Database   string `short:"d" long:"database" required:"true" description:"database name"`
	Engine     string `short:"e" long:"engine" required:"true" description:"engine name"`
	File       string `short:"f" long:"file" required:"true" description:"rel source file"`
	Relation   string `short:"r" long:"relation" description:"relation name (default: file name)"`
	HeaderRow  int    `long:"headerrow" default:"1" description:"header row number, 0 for no header"`
	Delim      string `long:"delim" description:"field delimiter"`
	EscapeChar string `long:"escapechar" description:"character used to escape quotes"`
	QuoteChar  string `long:"quotechar" description:"quoted field character"`
	Profile    string `long:"profile" default:"default" description:"config profile"`
}

func asRune(s string) rune {
	if s == "" {
		return 0
	}
	return []rune(s)[0]
}

// Returns the filename without path and extension.
func sansext(fname string) string {
	return strings.TrimSuffix(filepath.Base(fname), filepath.Ext(fname))
}

func run(opts *Options) error {
	client, err := rai.NewClientFromConfig(opts.Profile)
	if err != nil {
		return err
	}
	r, err := os.Open(opts.File)
	if err != nil {
		return err
	}
	rel := opts.Relation
	if rel == "" {
		rel = sansext(opts.File)
	}
	csv := &rai.CSVOptions{
		HeaderRow:  &opts.HeaderRow,
		Delim:      asRune(opts.Delim),
		EscapeChar: asRune(opts.EscapeChar),
		QuoteChar:  asRune(opts.QuoteChar)}
	rsp, err := client.LoadCSV(opts.Database, opts.Engine, rel, r, csv)
	if err != nil {
		return err
	}
	rsp.Show()
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
