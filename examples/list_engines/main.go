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
	"context"
	"flag"

	"github.com/relationalai/rai-sdk-go/rai"
)

func main() {
	state := flag.String("state", "", "state filter (default: none)")
	profile := flag.String("profile", rai.DefaultConfigProfile, "config profile (default: default)")
	flag.Parse()

	var cfg rai.Config
	rai.LoadConfigProfile(*profile, &cfg)
	client := rai.NewClient(context.Background(), rai.ClientOptions{Config: *cfg})
	rsp := client.ListEngines(state)
	rsp.Print()
}
