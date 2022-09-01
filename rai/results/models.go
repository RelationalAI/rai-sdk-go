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

package results

import (
	"github.com/apache/arrow/go/v9/arrow"
	"github.com/relationalai/rai-sdk-go/rai/pb"
)

type ColumnDefOld struct {
	TypeDef    map[string]interface{}
	Metadata   string
	ArrowIndex int
}

type ColumnDef struct {
	TypeDef    map[string]interface{}
	Metadata   pb.RelType
	ArrowIndex int
}

type ResultColumn struct {
	Array   []interface{}
	TypeDef map[string]interface{}
	Length  int
}

type ResultTable struct {
	RelationID string
	Record     arrow.Record // arrow.Record
	ColDefs    []ColumnDef
}