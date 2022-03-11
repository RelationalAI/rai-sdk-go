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
	"os"
)

// Helpers for encoding responses, including transaction results.

func makeIndent(indent int) string {
	result := make([]rune, indent)
	for i := 0; i < indent; i++ {
		result[i] = ' '
	}
	return string(result)
}

// Encode the given item as JSON to the given writer.
func Encode(w io.Writer, item interface{}, indent int) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", makeIndent(indent))
	return enc.Encode(item)
}

// Print the given item as JSON to stdout.
func Print(item interface{}, indent int) error {
	return Encode(os.Stdout, item, indent)
}
