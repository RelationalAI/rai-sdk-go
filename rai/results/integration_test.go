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
	"math/big"
	"testing"
	"time"

	"github.com/relationalai/rai-sdk-go/rai"
	"github.com/stretchr/testify/assert"
)

type test struct {
	Name     string
	Query    string
	TypeDefs []map[string]interface{}
	Values   []interface{}
	Skip     bool
}

func createClient() (*rai.Client, error) {
	return rai.NewClientFromConfig("default")
}

func TestStandardTypesIntegration(t *testing.T) {
	client, err := createClient()
	if err != nil {
		panic(err)
	}

	for _, test := range standardTypeTests {
		if !test.Skip {
			rsp, err := client.Execute("hnr-db", "hnr-engine", test.Query, nil, false)
			if err != nil {
				panic(err)
			}

			table := NewResultTable(rsp.Results[0])
			typeDefs := table.TypeDefs()
			values := table.Get(0)

			assert.Equal(t, typeDefs, test.TypeDefs)
			assert.Equal(t, values, test.Values)
			t.Logf("test: %s, OK", test.Name)
		}

	}
}

var standardTypeTests = []test{
	{
		"String",
		`def output = "test"`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "String"},
		},
		[]interface{}{"output", "test"},
		false,
	},
	{
		"Bool",
		`def output = boolean_true, boolean_false`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "Bool"},
			{"type": "Bool"},
		},
		[]interface{}{"output", true, false},
		false,
	},
	{
		"Char",
		`def output = 'a', '👍'`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "Char"},
			{"type": "Char"},
		},
		[]interface{}{"output", "a", "👍"},
		false,
	},
	{
		"DateTime",
		`def output = 2021-10-12T01:22:31+10:00`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "DateTime"},
		},
		[]interface{}{"output", "2021-10-11T16:22:31+01:00"},
		false,
	},
	{
		"Date",
		`def output = 2021-10-12`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "Date"},
		},
		[]interface{}{"output", "2021-10-12"},
		false,
	},
	{
		"Year",
		`def output = Year[2022]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "Year"},
		},
		[]interface{}{"output", int64(2022)},
		false,
	},
	{
		"Month",
		`def output = Month[1]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "Month"},
		},
		[]interface{}{"output", time.Month(1)},
		false,
	},
	{
		"Week",
		`def output = Week[1]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "Week"},
		},
		[]interface{}{"output", int64(1)},
		false,
	},
	{
		"Day",
		`def output = Day[1]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "Day"},
		},
		[]interface{}{"output", int64(1)},
		false,
	},
	{
		"Hour",
		`def output = Hour[1]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "Hour"},
		},
		[]interface{}{"output", int64(1)},
		false,
	},
	{
		"Minute",
		`def output = Minute[1]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "Minute"},
		},
		[]interface{}{"output", int64(1)},
		false,
	},
	{
		"Second",
		`def output = Second[1]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "Second"},
		},
		[]interface{}{"output", int64(1)},
		false,
	},
	{
		"Millisecond",
		`def output = Millisecond[1]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "Millisecond"},
		},
		[]interface{}{"output", int64(1)},
		false,
	},
	{
		"Microsecond",
		`def output = Microsecond[1]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "Microsecond"},
		},
		[]interface{}{"output", int64(1)},
		false,
	},
	{
		"Nanosecond",
		`def output = Nanosecond[1]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "Nanosecond"},
		},
		[]interface{}{"output", int64(1)},
		false,
	},
	{
		"Hash",
		`
		entity type Foo = Int
		def output = ^Foo[12]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "Hash"},
		},
		[]interface{}{"output", strToBig("290925887971139297379988470542779955742")},
		false,
	},
	{
		"Missing",
		`def output = missing`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "Missing"},
		},
		[]interface{}{"output", nil},
		false,
	},
	{
		"FilePos",
		`
		def config:data = """
		a,b,c
		1,2,3
		4,5,6
		"""

		def csv = load_csv[config]

		def output(p) = csv(_, p, _)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "FilePos"},
		},
		[]interface{}{"output", int64(2)},
		false,
	},
	{
		"Int8",
		`def output = int[8, 12], int[8, -12]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "Int8"}, {"type": "Int8"},
		},
		[]interface{}{"output", int8(12), int8(-12)},
		false,
	},
	{
		"Int16",
		`def output = int[16, 123], int[16, -123]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "Int16"}, {"type": "Int16"},
		},
		[]interface{}{"output", int16(123), int16(-123)},
		false,
	},
	{
		"Int32",
		`def output = int[32, 1234], int[32, -1234]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "Int32"}, {"type": "Int32"},
		},
		[]interface{}{"output", int32(1234), int32(-1234)},
		false,
	},
	{
		"Int64",
		`def output = int[64, 12345], int[64, -12345]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "Int64"}, {"type": "Int64"},
		},
		[]interface{}{"output", int64(12345), int64(-12345)},
		false,
	},
	{ // FIXME: negative int128 are not correctly parsed
		"Int128",
		`def output = 123456789101112131415, int[128, 0], int[128, -10^10]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "Int128"}, {"type": "Int128"}, {"type": "Int128"},
		},
		[]interface{}{"output", strToBig("123456789101112131415"), new(big.Int).SetBits([]big.Word{0, 0}), strToBig("-10000000000")},
		true,
	},
	{
		"UInt8",
		`def output = uint[8, 12]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "UInt8"},
		},
		[]interface{}{"output", uint8(12)},
		false,
	},
	{
		"UInt16",
		`def output = uint[16, 123]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "UInt16"},
		},
		[]interface{}{"output", uint16(123)},
		false,
	},
	{
		"UInt32",
		`def output = uint[32, 1234]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "UInt32"},
		},
		[]interface{}{"output", uint32(1234)},
		false,
	},
	{
		"UInt64",
		`def output = uint[64, 12345]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "UInt64"},
		},
		[]interface{}{"output", uint64(12345)},
		false,
	},
	{
		"UInt128",
		`def output = uint[128, 123456789101112131415], uint[128, 0], 0xdade49b564ec827d92f4fd30f1023a1e`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "UInt128"}, {"type": "UInt128"}, {"type": "UInt128"},
		},
		[]interface{}{"output", strToBig("123456789101112131415"), new(big.Int).SetBits([]big.Word{0, 0}), strToBig("290925887971139297379988470542779955742")},
		false,
	},
}
