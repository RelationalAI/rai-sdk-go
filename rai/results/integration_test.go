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
	"github.com/shopspring/decimal"
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
			rsp, err := client.Execute("hnr-db", "hnr-engine", test.Query, nil, true)
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

func TestSpecializationIntegration(t *testing.T) {
	client, err := createClient()
	if err != nil {
		panic(err)
	}

	for _, test := range specializationTests {
		if !test.Skip {
			rsp, err := client.Execute("hnr-db", "hnr-engine", test.Query, nil, true)
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

func TestValueTypesIntegration(t *testing.T) {
	client, err := createClient()
	if err != nil {
		panic(err)
	}

	for _, test := range valueTypeTests {
		if !test.Skip {
			rsp, err := client.Execute("hnr-db", "hnr-engine", test.Query, nil, true)
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

func TestMiscValueTypeIntegration(t *testing.T) {
	client, err := createClient()
	if err != nil {
		panic(err)
	}

	for _, test := range miscValueTypeTests {
		if !test.Skip {
			rsp, err := client.Execute("hnr-db", "hnr-engine", test.Query, nil, true)
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
	{
		"Float16",
		`def output = float[16, 12], float[16, 42.5]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "Float16"}, {"type": "Float16"},
		},
		[]interface{}{"output", float32(12.0), float32(42.5)},
		false,
	},
	{
		"Float32",
		`def output = float[32, 12], float[32, 42.5]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "Float32"}, {"type": "Float32"},
		},
		[]interface{}{"output", float32(12.0), float32(42.5)},
		false,
	},
	{
		"Float64",
		`def output = float[64, 12], float[64, 42.5]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "Float64"}, {"type": "Float64"},
		},
		[]interface{}{"output", float64(12.0), float64(42.5)},
		false,
	},
	{
		"Decimal16",
		`def output = parse_decimal[16, 2, "12.34"]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"places": "2", "type": "Decimal16"},
		},
		[]interface{}{"output", decimal.New(1234, -2)},
		false,
	},
	{
		"Decimal32",
		`def output = parse_decimal[32, 2, "12.34"]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"places": "2", "type": "Decimal32"},
		},
		[]interface{}{"output", decimal.New(1234, -2)},
		false,
	},
	{
		"Decimal64",
		`def output = parse_decimal[64, 2, "12.34"]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"places": "2", "type": "Decimal64"},
		},
		[]interface{}{"output", decimal.New(1234, -2)},
		false,
	},
	{ //FIXME: decimal package doesn't support big.Int
		"Decimal64",
		`def output = parse_decimal[128, 2, "12345678901011121314.34"]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"places": "2", "type": "Decimal128"},
		},
		[]interface{}{"output", decimal.New(1234, -2)},
		true,
	},
	{
		"Rational8",
		`def output = rational[8, 1, 2]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "Rational8"},
		},
		[]interface{}{"output", big.NewRat(1, 2)},
		false,
	},
	{
		"Rational16",
		`def output = rational[16, 1, 2]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "Rational16"},
		},
		[]interface{}{"output", big.NewRat(1, 2)},
		false,
	},
	{
		"Rational32",
		`def output = rational[32, 1, 2]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "Rational32"},
		},
		[]interface{}{"output", big.NewRat(1, 2)},
		false,
	},
	{
		"Rational64",
		`def output = rational[64, 1, 2]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "Rational64"},
		},
		[]interface{}{"output", big.NewRat(1, 2)},
		false,
	},
	{ // FIXME: big.NewRat don't support big.Int as value
		"Rational128",
		`def output = rational[128, 123456789101112313, 9123456789101112313]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "Rational128"},
		},
		[]interface{}{"output", big.NewRat(strToBig("9123456789101112313").Int64(), strToBig("123456789101112313").Int64())},
		true,
	},
}

var specializationTests = []test{
	{
		"String(symbol)",
		`def output= :foo`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "foo",
				},
			},
		},
		[]interface{}{"output", "foo"},
		false,
	},
	{
		"String",
		`
		def v = "foo"
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "foo",
				},
			},
		},
		[]interface{}{"output", "foo"},
		false,
	},
	{
		"String with slash",
		`
		def v = "foo / bar"
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "foo / bar",
				},
			},
		},
		[]interface{}{"output", "foo / bar"},
		false,
	},
	{
		"Char",
		`
		def v = '👍'
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "Char",
					"value": "👍",
				},
			},
		},
		[]interface{}{"output", "👍"},
		false,
	},
	{
		"DateTime",
		`
		def v = 2021-10-12T01:22:31+10:00
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "DateTime",
					"value": "2021-10-11T16:22:31+01:00",
				},
			},
		},
		[]interface{}{"output", "2021-10-11T16:22:31+01:00"},
		true, // enable back when DateTime serialization is fixed
	},
	{
		"Date",
		`
		def v = 2021-10-12
      	def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "Date",
					"value": "2021-10-12",
				},
			},
		},
		[]interface{}{"output", "2021-10-12"},
		false,
	},
	{
		"Year",
		`
		def v = Year[2022]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "Year",
					"value": int64(2022),
				},
			},
		},
		[]interface{}{"output", int64(2022)},
		false,
	},
	{
		"Month",
		`
		def v = Month[1]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "Month",
					"value": time.Month(1),
				},
			},
		},
		[]interface{}{"output", time.Month(1)},
		false,
	},
	{
		"Week",
		`
		def v = Week[1]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "Week",
					"value": int64(1),
				},
			},
		},
		[]interface{}{"output", int64(1)},
		false,
	},
	{
		"Day",
		`
		def v = Day[1]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "Day",
					"value": int64(1),
				},
			},
		},
		[]interface{}{"output", int64(1)},
		false,
	},
	{
		"Hour",
		`
		def v = Hour[1]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "Hour",
					"value": int64(1),
				},
			},
		},
		[]interface{}{"output", int64(1)},
		false,
	},
	{
		"Minute",
		`
		def v = Minute[1]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "Minute",
					"value": int64(1),
				},
			},
		},
		[]interface{}{"output", int64(1)},
		false,
	},
	{
		"Second",
		`
		def v = Second[1]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "Second",
					"value": int64(1),
				},
			},
		},
		[]interface{}{"output", int64(1)},
		false,
	},
	{
		"Millisecond",
		`
		def v = Millisecond[1]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "Millisecond",
					"value": int64(1),
				},
			},
		},
		[]interface{}{"output", int64(1)},
		false,
	},
	{
		"Microsecond",
		`
		def v = Microsecond[1]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "Microsecond",
					"value": int64(1),
				},
			},
		},
		[]interface{}{"output", int64(1)},
		false,
	},
	{
		"Nanosecond",
		`
		def v = Nanosecond[1]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "Nanosecond",
					"value": int64(1),
				},
			},
		},
		[]interface{}{"output", int64(1)},
		false,
	},
	{
		"Hash",
		`
		entity type Foo = Int
		def v = ^Foo[12]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "Hash",
					"value": strToBig("290925887971139297379988470542779955742"),
				},
			},
		},
		[]interface{}{"output", strToBig("290925887971139297379988470542779955742")},
		false,
	},
	{
		"Missing",
		`
		def v = missing
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "Missing",
					"value": nil,
				},
			},
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

		def v(p) = csv(_, p, _)
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "FilePos",
					"value": int64(2),
				},
			},
		},
		[]interface{}{"output", int64(2)},
		false,
	},
	{
		"Int8",
		`
		def v = int[8, -12]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "Int8",
					"value": int32(-12),
				},
			},
		},
		[]interface{}{"output", int32(-12)},
		false,
	},
	{
		"Int16",
		`
		def v = int[16, -123]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "Int16",
					"value": int32(-123),
				},
			},
		},
		[]interface{}{"output", int32(-123)},
		false,
	},
	{
		"Int32",
		`
		def v = int[32, -1234]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "Int32",
					"value": int32(-1234),
				},
			},
		},
		[]interface{}{"output", int32(-1234)},
		false,
	},
	{
		"Int64",
		`
		def v = int[64, -12345]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "Int64",
					"value": int64(-12345),
				},
			},
		},
		[]interface{}{"output", int64(-12345)},
		false,
	},
	{ // FIXME: negative int128 are not correctly parsed
		"Int128",
		`
		def v = int[128, 123456789101112131415]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "Int128",
					"value": strToBig("123456789101112131415"),
				},
			},
		},
		[]interface{}{"output", strToBig("123456789101112131415")},
		false,
	},
	{
		"UInt8",
		`
		def v = uint[8, 12]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "UInt8",
					"value": uint32(12),
				},
			},
		},
		[]interface{}{"output", uint32(12)},
		false,
	},
	{
		"UInt16",
		`
		def v = uint[16, 123]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "UInt16",
					"value": uint32(123),
				},
			},
		},
		[]interface{}{"output", uint32(123)},
		false,
	},
	{
		"UInt32",
		`
		def v = uint[32, 1234]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "UInt32",
					"value": uint32(1234),
				},
			},
		},
		[]interface{}{"output", uint32(1234)},
		false,
	},
	{
		"UInt64",
		`
		def v = uint[64, 12345]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "UInt64",
					"value": uint64(12345),
				},
			},
		},
		[]interface{}{"output", uint64(12345)},
		false,
	},
	{
		"UInt128",
		`
		def v = uint[128, 123456789101112131415]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "UInt128",
					"value": strToBig("123456789101112131415"),
				},
			},
		},
		[]interface{}{"output", strToBig("123456789101112131415")},
		false,
	},
	{
		"Float16",
		`
		def v = float[16, 42.5]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "Float16",
					"value": float32(42.5),
				},
			},
		},
		[]interface{}{"output", float32(42.5)},
		false,
	},
	{
		"Float32",
		`
		def v = float[32, 42.5]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "Float32",
					"value": float32(42.5),
				},
			},
		},
		[]interface{}{"output", float32(42.5)},
		false,
	},
	{
		"Float64",
		`
		def v = float[64, 42.5]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "Float64",
					"value": float64(42.5),
				},
			},
		},
		[]interface{}{"output", float64(42.5)},
		false,
	},
	{
		"Decimal16",
		`
		def v = parse_decimal[16, 2, "12.34"]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"places": "2",
					"type":   "Decimal16",
					"value":  decimal.New(1234, -2),
				},
			},
		},
		[]interface{}{"output", decimal.New(1234, -2)},
		false,
	},
	{
		"Decimal32",
		`
		def v = parse_decimal[32, 2, "12.34"]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"places": "2",
					"type":   "Decimal32",
					"value":  decimal.New(1234, -2),
				},
			},
		},
		[]interface{}{"output", decimal.New(1234, -2)},
		false,
	},
	{
		"Decimal64",
		`
		def v = parse_decimal[64, 2, "12.34"]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"places": "2",
					"type":   "Decimal64",
					"value":  decimal.New(1234, -2),
				},
			},
		},
		[]interface{}{"output", decimal.New(1234, -2)},
		false,
	},
	{ //FIXME: decimal package doesn't support big.Int
		"Decimal128",
		`def output = parse_decimal[128, 2, "12345678901011121314.34"]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"places": "2",
					"type":   "Decimal128",
					"value":  decimal.New(1234, -2),
				},
			},
		},
		[]interface{}{"output", decimal.New(1234, -2)},
		true,
	},
	{
		"Rational8",
		`
		def v = rational[8, 1, 2]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "Rational8",
					"value": big.NewRat(1, 2),
				},
			},
		},
		[]interface{}{"output", big.NewRat(1, 2)},
		false,
	},
	{
		"Rational16",
		`
		def v = rational[16, 1, 2]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "Rational16",
					"value": big.NewRat(1, 2),
				},
			},
		},
		[]interface{}{"output", big.NewRat(1, 2)},
		false,
	},
	{
		"Rational32",
		`
		def v = rational[32, 1, 2]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "Rational32",
					"value": big.NewRat(1, 2),
				},
			},
		},
		[]interface{}{"output", big.NewRat(1, 2)},
		false,
	},
	{
		"Rational64",
		`
		def v = rational[64, 1, 2]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "Rational64",
					"value": big.NewRat(1, 2),
				},
			},
		},
		[]interface{}{"output", big.NewRat(1, 2)},
		false,
	},
	{ // FIXME: big.NewRat don't support big.Int as value
		"Rational128",
		`def output = rational[128, 123456789101112313, 9123456789101112313]`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{"type": "Rational128"},
		},
		[]interface{}{"output", big.NewRat(strToBig("9123456789101112313").Int64(), strToBig("123456789101112313").Int64())},
		true,
	},
}

var valueTypeTests = []test{
	{
		"String(symbol)",
		`
		value type MyType = :foo; :bar; :baz
		def output = ^MyType[:foo]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "foo",
						},
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", "foo"}},
		false,
	},
	{
		"String",
		`
		value type MyType = Int, String
		def output = ^MyType[1, "abc"]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "String",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), "abc"}},
		false,
	},
	{
		"Bool",
		`
		value type MyType = Int, Boolean
		def output = ^MyType[1, boolean_false]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "Bool",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), false}},
		false,
	},
	{
		"Char",
		`
		value type MyType = Int, Char
		def output = ^MyType[1, '👍']
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "Char",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), "👍"}},
		false,
	},
	{
		"DateTime",
		`
		value type MyType = Int, DateTime
		def output = ^MyType[1, 2021-10-12T01:22:31+10:00]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "DateTime",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), "2021-10-11T16:22:31+01:00"}},
		false,
	},
	{
		"Date",
		`
		value type MyType = Int, Date
		def output = ^MyType[1, 2021-10-12]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "Date",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), "2021-10-12"}},
		false,
	},
	{
		"Year",
		`
		value type MyType = Int, is_Year
		def output = ^MyType[1, Year[2022]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "Year",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), int64(2022)}},
		false,
	},
	{
		"Month",
		`
		value type MyType = Int, is_Month
		def output = ^MyType[1, Month[2]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "Month",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), time.Month(2)}},
		false,
	},
	{
		"Week",
		`
		value type MyType = Int, is_Week
		def output = ^MyType[1, Week[2]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "Week",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), int64(2)}},
		false,
	},
	{
		"Day",
		`
		value type MyType = Int, is_Day
		def output = ^MyType[1, Day[2]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "Day",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), int64(2)}},
		false,
	},
	{
		"Hour",
		`
		value type MyType = Int, is_Hour
		def output = ^MyType[1, Hour[2]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "Hour",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), int64(2)}},
		false,
	},
	{
		"Minute",
		`
		value type MyType = Int, is_Minute
		def output = ^MyType[1, Minute[2]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "Minute",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), int64(2)}},
		false,
	},
	{
		"Second",
		`
		value type MyType = Int, is_Second
		def output = ^MyType[1, Second[2]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "Second",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), int64(2)}},
		false,
	},
	{
		"Millisecond",
		`
		value type MyType = Int, is_Millisecond
		def output = ^MyType[1, Millisecond[2]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "Millisecond",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), int64(2)}},
		false,
	},
	{
		"Microsecond",
		`
		value type MyType = Int, is_Microsecond
		def output = ^MyType[1, Microsecond[2]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "Microsecond",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), int64(2)}},
		false,
	},
	{
		"Nanosecond",
		`
		value type MyType = Int, is_Nanosecond
		def output = ^MyType[1, Nanosecond[2]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "Nanosecond",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), int64(2)}},
		false,
	},
	{
		"Hash",
		`
		value type MyType = Int, Hash
		def h(x) = hash128["abc", _, x]
		def output = ^MyType[1, h]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "Hash",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), strToBig("59005302613613978016770438099762432572")}},
		false,
	},
	{
		"Missing",
		`
		value type MyType = Int, Missing
		def output = ^MyType[1, missing]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "Missing",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), nil}},
		false,
	},
	{
		"FilePos",
		`
		def config:data="""
		a,b,c
		1,2,3
		"""

		def csv = load_csv[config]
		def v(p) = csv(_, p, _)
		value type MyType = Int, FilePos
		def output = ^MyType[1, v]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "FilePos",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), int64(2)}},
		false,
	},
	{
		"Int8",
		`
		value type MyType = Int, SignedInt[8]
      	def output = ^MyType[1, int[8, -12]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "Int8",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), int8(-12)}},
		false,
	},
	{
		"Int16",
		`
		value type MyType = Int, SignedInt[16]
      	def output = ^MyType[1, int[16, -123]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "Int16",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), int16(-123)}},
		false,
	},
	{
		"Int32",
		`
		value type MyType = Int, SignedInt[32]
      	def output = ^MyType[1, int[32, -1234]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "Int32",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), int32(-1234)}},
		false,
	},
	{
		"Int64",
		`
		value type MyType = Int, SignedInt[64]
      	def output = ^MyType[1, int[64, -12345]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "Int64",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), int64(-12345)}},
		false,
	},
	{
		"Int128",
		`
		value type MyType = Int, SignedInt[128]
		def output = ^MyType[1, int[128, 123456789101112131415]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "Int128",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), strToBig("123456789101112131415")}},
		false,
	},
	{
		"UInt8",
		`
		value type MyType = Int, UnsignedInt[8]
		def output = ^MyType[1, uint[8, 12]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "UInt8",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), uint8(12)}},
		false,
	},
	{
		"UInt16",
		`
		value type MyType = Int, UnsignedInt[16]
		def output = ^MyType[1, uint[16, 123]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "UInt16",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), uint16(123)}},
		false,
	},
	{
		"UInt32",
		`
		value type MyType = Int, UnsignedInt[32]
		def output = ^MyType[1, uint[32, 1234]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "UInt32",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), uint32(1234)}},
		false,
	},
	{
		"UInt64",
		`
		value type MyType = Int, UnsignedInt[64]
		def output = ^MyType[1, uint[64, 12345]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "UInt64",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), uint64(12345)}},
		false,
	},
	{
		"UInt128",
		`
		value type MyType = Int, UnsignedInt[128]
		def output = ^MyType[1, uint[128, 123456789101112131415]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "UInt128",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), strToBig("123456789101112131415")}},
		false,
	},
	{
		"Float16",
		`
		value type MyType = Int, Floating[16]
		def output = ^MyType[1, float[16, 42.5]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "Float16",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), float32(42.5)}},
		false,
	},
	{
		"Float32",
		`
		value type MyType = Int, Floating[32]
		def output = ^MyType[1, float[32, 42.5]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "Float32",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), float32(42.5)}},
		false,
	},
	{
		"Float64",
		`
		value type MyType = Int, Floating[64]
		def output = ^MyType[1, float[64, 42.5]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "Float64",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), float64(42.5)}},
		false,
	},
	{
		"Decimal16",
		`
		value type MyType = Int, FixedDecimal[16, 2]
		def output = ^MyType[1, parse_decimal[16, 2, "12.34"]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"places": "2",
						"type":   "Decimal16",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), decimal.New(1234, -2)}},
		false,
	},
	{
		"Decimal32",
		`
		value type MyType = Int, FixedDecimal[32, 2]
		def output = ^MyType[1, parse_decimal[32, 2, "12.34"]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"places": "2",
						"type":   "Decimal32",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), decimal.New(1234, -2)}},
		false,
	},
	{
		"Decimal64",
		`
		value type MyType = Int, FixedDecimal[64, 2]
		def output = ^MyType[1, parse_decimal[64, 2, "12.34"]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"places": "2",
						"type":   "Decimal64",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), decimal.New(1234, -2)}},
		false,
	},
	{
		"Decimal128",
		`
		value type MyType = Int, FixedDecimal[128, 2]
		def output = ^MyType[1, parse_decimal[128, 2, "12345678901011121314.34"]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"places": "2",
						"type":   "Decimal64",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), decimal.New(123456789010111213, -2)}},
		true,
	},
	{
		"Rational8",
		`
		value type MyType = Int, Rational[8]
		def output = ^MyType[1, rational[8, 1, 2]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "Rational8",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), big.NewRat(1, 2)}},
		false,
	},
	{
		"Rational16",
		`
		value type MyType = Int, Rational[16]
		def output = ^MyType[1, rational[16, 1, 2]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "Rational16",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), big.NewRat(1, 2)}},
		false,
	},
	{
		"Rational32",
		`
		value type MyType = Int, Rational[32]
		def output = ^MyType[1, rational[32, 1, 2]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "Rational32",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), big.NewRat(1, 2)}},
		false,
	},
	{
		"Rational64",
		`
		value type MyType = Int, Rational[64]
		def output = ^MyType[1, rational[64, 1, 2]]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "Rational64",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), big.NewRat(1, 2)}},
		false,
	},
	{
		"Rational128",
		`
		value type MyType = Int, Rational[128]
		def output = ^MyType[1, rational[128, 123456789101112313, 9123456789101112313]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "Rational128",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(1), big.NewRat(1, 2)}},
		true,
	},
}

var miscValueTypeTests = []test{
	{
		"Int",
		`
		value type MyType = Int
		def output = ^MyType[123]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", int64(123)}},
		false,
	},
	{ // FixMe: big.Int wrong conversion
		"Int128",
		`
		value type MyType = SignedInt[128]
		def output = ^MyType[123445677777999999999]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int128",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", strToBig("123445677777999999999")}},
		true,
	},
	{
		"Date",
		`
		value type MyType = Date
		def output = ^MyType[2021-10-12]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Date",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", "2021-10-12"}},
		false,
	},
	{
		"OuterType(InnerType(Int, String), String)",
		`
		value type InnerType = Int, String
		value type OuterType = InnerType, String
		def output = ^OuterType[^InnerType[123, "inner"], "outer"]
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "OuterType",
						},
					}, map[string]interface{}{
						"type": "ValueType",
						"typeDefs": []interface{}{
							map[string]interface{}{
								"type": "Constant",
								"value": map[string]interface{}{
									"type":  "String",
									"value": "InnerType",
								},
							},
							map[string]interface{}{
								"type": "Int64",
							},
							map[string]interface{}{
								"type": "String",
							},
						},
					},
					map[string]interface{}{"type": "String"},
				},
			},
		},
		[]interface{}{"output", []interface{}{"OuterType", []interface{}{"InnerType", int64(123), "inner"}, "outer"}},
		false,
	},
	{
		"Module",
		`
		module Foo
        	module Bar
          		value type MyType = Int, Int
        	end
      	end
      	def output = Foo:Bar:^MyType[12, 34]
		`,
		[]map[string]interface{}{
			map[string]interface{}{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			map[string]interface{}{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "Foo",
						},
					},
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "Bar",
						},
					},
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Int64",
					},
					map[string]interface{}{
						"type": "Int64",
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"Foo", "Bar", "MyType", int64(12), int64(34)}},
		false,
	},
	{ // FIXME: enable this when specialization on value types isfixed
		"String(symbol)",
		`
		value type MyType = :foo; :bar; :baz
		def v = ^MyType[:foo]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "ValueType",
				"typeDefs": []interface{}{
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "MyType",
						},
					},
					map[string]interface{}{
						"type": "Constant",
						"value": map[string]interface{}{
							"type":  "String",
							"value": "foo",
						},
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{"MyType", "foo"}},
		true,
	},
	{
		"Int",
		`
		value type MyType = Int
		def v = ^MyType[123]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "Int64",
					"value": int64(123),
				},
			},
		},
		[]interface{}{"output", int64(123)},
		false,
	},
	{
		"Int, Int",
		`
		value type MyType = Int, Int
		def v = ^MyType[123, 456]
		def output = #(v)
		`,
		[]map[string]interface{}{
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type":  "String",
					"value": "output",
				},
			},
			{
				"type": "Constant",
				"value": map[string]interface{}{
					"type": "ValueType",
					"typeDefs": []interface{}{
						map[string]interface{}{
							"type": "Int64",
						},
						map[string]interface{}{
							"type": "Int64",
						},
					},
					"value": []interface{}{
						int64(123), int64(456),
					},
				},
			},
		},
		[]interface{}{"output", []interface{}{int64(123), int64(456)}},
		false,
	},
}
