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

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

type testInput struct {
	RelType     string
	Type        string
	Places      interface{}
	Query       string
	ArrowValues []interface{}
	Values      []interface{}
	Skip        bool
}

func TestConvertValue(t *testing.T) {
	for _, testInput := range testInputs {
		if !testInput.Skip {
			typeDef := make(map[string]interface{})

			typeDef["type"] = testInput.Type
			if testInput.Places != nil {
				typeDef["places"] = testInput.Places
			}

			for i, val := range testInput.ArrowValues {
				v, err := convertValue(typeDef, val)
				t.Logf("test: %s", testInput.Type)
				assert.Nil(t, err)
				assert.Equal(t, v, testInput.Values[i])
				t.Logf("test: %s, OK", testInput.Type)
			}
		}
	}
}

var testInputs = []testInput{
	{
		"String",
		"String",
		nil,
		"def output = \"test\"",
		[]interface{}{"test"},
		[]interface{}{"test"},
		false,
	},
	{
		"Bool",
		"Bool",
		nil,
		"def output = boolean_true, boolean_false",
		[]interface{}{true, false},
		[]interface{}{true, false},
		false,
	},
	{
		"Char",
		"Char",
		nil,
		"def output = 'a', '👍'",
		[]interface{}{uint32(97), uint32(128077)},
		[]interface{}{"a", "👍"},
		false,
	},
	{
		"Dates.DateTime",
		"DateTime",
		nil,
		"def output = 2021-10-12T01:22:31+10:00",
		[]interface{}{int64(63769648951000)},
		[]interface{}{"2021-10-11T16:22:31+01:00"},
		false,
	},
	{
		"Dates.Date",
		"Date",
		nil,
		"def output = 2021-10-12",
		[]interface{}{int64(738075)},
		[]interface{}{"2021-10-12"},
		false,
	},
	{
		"Dates.Year",
		"Year",
		nil,
		"def output = Year[2022]",
		[]interface{}{int64(2022)},
		[]interface{}{int64(2022)},
		false,
	},
	{
		"Dates.Month",
		"Month",
		nil,
		"def output = Month[1]",
		[]interface{}{int64(1)},
		[]interface{}{time.Month(1)},
		false,
	},
	{
		"Dates.Week",
		"Week",
		nil,
		"def output = Week[1]",
		[]interface{}{int64(1)},
		[]interface{}{int64(1)},
		false,
	},
	{
		"Dates.Day",
		"Day",
		nil,
		"def output = Day[1]",
		[]interface{}{int64(1)},
		[]interface{}{int64(1)},
		false,
	},
	{
		"Dates.Hour",
		"Hour",
		nil,
		"def output = Hour[1]",
		[]interface{}{int64(1)},
		[]interface{}{int64(1)},
		false,
	},
	{
		"Dates.Minute",
		"Minute",
		nil,
		"def output = Minute[1]",
		[]interface{}{int64(1)},
		[]interface{}{int64(1)},
		false,
	},
	{
		"Dates.Second",
		"Second",
		nil,
		"def output = Second[1]",
		[]interface{}{int64(1)},
		[]interface{}{int64(1)},
		false,
	},
	{
		"Dates.Millisecond",
		"Millisecond",
		nil,
		"def output = Millisecond[1]",
		[]interface{}{int64(1)},
		[]interface{}{int64(1)},
		false,
	},
	{
		"Dates.Microsecond",
		"Microsecond",
		nil,
		"def output = Microsecond[1]",
		[]interface{}{int64(1)},
		[]interface{}{int64(1)},
		false,
	},
	{
		"Dates.Nanosecond",
		"Nanosecond",
		nil,
		"def output = Nanosecond[1]",
		[]interface{}{int64(1)},
		[]interface{}{int64(1)},
		false,
	},
	{
		"HashValue",
		"Hash",
		nil,
		`entity type Foo = Int
		def output = ^Foo[12]`,
		[]interface{}{[]interface{}{uint64(10589367010498591262), uint64(15771123988529185405)}},
		[]interface{}{strToBig("290925887971139297379988470542779955742")},
		false,
	},
	{
		"Missing",
		"Missing",
		nil,
		"def output = missing",
		[]interface{}{"{}"},
		[]interface{}{nil},
		false,
	},
	{
		"FilePos",
		"FilePos",
		nil,
		`
		def config:data = """
		a,b,c
		1,2,3
		4,5,6
		"""

		def csv = load_csv[config]

		def output(p) = csv(_, p, _)
		`,
		[]interface{}{int64(2)},
		[]interface{}{int64(2)},
		false,
	},
	{
		"Int8",
		"Int8",
		nil,
		"def output = int[8, 12], int[8, -12]",
		[]interface{}{int8(12), int8(-12)},
		[]interface{}{int8(12), int8(-12)},
		false,
	},
	{
		"Int16",
		"Int16",
		nil,
		"def output = int[16, 12], int[16, -12]",
		[]interface{}{int16(12), int16(-12)},
		[]interface{}{int16(12), int16(-12)},
		false,
	},
	{
		"Int32",
		"Int32",
		nil,
		"def output = int[32, 12], int[32, -12]",
		[]interface{}{int32(12), int32(-12)},
		[]interface{}{int32(12), int32(-12)},
		false,
	},
	{
		"Int64",
		"Int64",
		nil,
		"def output = int[64, 12], int[64, -12]",
		[]interface{}{int64(12), int64(-12)},
		[]interface{}{int64(12), int64(-12)},
		false,
	}, // FIXME: negative int128 are not correctly parsed
	{
		"Int128",
		"Int128",
		nil,
		"def output = 123456789101112131415, int[128, 0], int[128, -10^10]",
		[]interface{}{[]interface{}{uint64(12776324658854821719), uint64(6)}, []interface{}{uint64(0), uint64(0)}, []interface{}{uint64(18446744063709551616), uint64(18446744073709551615)}},
		[]interface{}{strToBig("123456789101112131415"), new(big.Int).SetBits([]big.Word{0, 0}), strToBig("-10000000000")},
		true,
	},
	{
		"UInt8",
		"UInt8",
		nil,
		"def output = uint[8, 12]",
		[]interface{}{uint8(12)},
		[]interface{}{uint8(12)},
		false,
	},
	{
		"UInt16",
		"UInt16",
		nil,
		"def output = uint[16, 123]",
		[]interface{}{int16(123)},
		[]interface{}{int16(123)},
		false,
	},
	{
		"UInt32",
		"UInt32",
		nil,
		"def output = uint[32, 1234]",
		[]interface{}{uint32(1234)},
		[]interface{}{uint32(1234)},
		false,
	},
	{
		"UInt64",
		"UInt64",
		nil,
		"def output = uint[64, 12345]",
		[]interface{}{uint64(12345)},
		[]interface{}{uint64(12345)},
		false,
	},
	// FIXME: strToBig("0") is different from the parsed value
	{
		"UInt128",
		"UInt128",
		nil,
		"def output = uint[128, 123456789101112131415], uint[128, 0], 0xdade49b564ec827d92f4fd30f1023a1e",
		[]interface{}{[]interface{}{uint64(12776324658854821719), uint64(6)}, []interface{}{uint64(0), uint64(0)}, []interface{}{uint64(10589367010498591262), uint64(15771123988529185405)}},
		[]interface{}{strToBig("123456789101112131415"), new(big.Int).SetBits([]big.Word{0, 0}), strToBig("290925887971139297379988470542779955742")},
		false,
	},
	{
		"Float16",
		"Float16",
		nil,
		"def output = float[16, 12], float[16, 42.5]",
		[]interface{}{float32(12), float32(42.5)},
		[]interface{}{float32(12), float32(42.5)},
		false,
	},
	{
		"Float32",
		"Float32",
		nil,
		"def output = float[32, 12], float[32, 42.5]",
		[]interface{}{float32(12), float32(42.5)},
		[]interface{}{float32(12), float32(42.5)},
		false,
	},
	{
		"Float64",
		"Float64",
		nil,
		"def output = float[64, 12], float[64, 42.5]",
		[]interface{}{float64(12), float64(42.5)},
		[]interface{}{float64(12), float64(42.5)},
		false,
	},
	{
		"FixedPointDecimals.FixedDecimal{Int16, 2}",
		"Decimal16",
		"2",
		`def output = parse_decimal[16, 2, "12.34"]`,
		[]interface{}{int16(1234)},
		[]interface{}{decimal.New(1234, -2)},
		false,
	},
	{
		"FixedPointDecimals.FixedDecimal{Int32, 2}",
		"Decimal32",
		"2",
		`def output = parse_decimal[32, 2, "12.34"]`,
		[]interface{}{int32(1234)},
		[]interface{}{decimal.New(1234, -2)},
		false,
	},
	{
		"FixedPointDecimals.FixedDecimal{Int64, 2}",
		"Decimal64",
		"2",
		`def output = parse_decimal[64, 2, "12.34"]`,
		[]interface{}{int64(1234)},
		[]interface{}{decimal.New(1234, -2)},
		false,
	},
	// FIXME: decimal package doesn't support big.Int
	{
		"FixedPointDecimals.FixedDecimal{Int128, 2}",
		"Decimal128",
		"2",
		`def output = parse_decimal[128, 2, "12345678901011121314.34"]`,
		[]interface{}{[]interface{}{uint64(17082781236281724778), uint64(66)}},
		[]interface{}{decimal.New(123, -2)},
		true,
	},
	{
		"Rational{Int8}",
		"Rational8",
		nil,
		"def output = rational[8, 1, 2]",
		[]interface{}{[]interface{}{int8(1), int8(2)}},
		[]interface{}{big.NewRat(1, 2)},
		false,
	},
	{
		"Rational{Int16}",
		"Rational16",
		nil,
		"def output = rational[16, 1, 2]",
		[]interface{}{[]interface{}{int16(1), int16(2)}},
		[]interface{}{big.NewRat(1, 2)},
		false,
	},
	{
		"Rational{Int32}",
		"Rational32",
		nil,
		"def output = rational[32, 1, 2]",
		[]interface{}{[]interface{}{int32(1), int32(2)}},
		[]interface{}{big.NewRat(1, 2)},
		false,
	},
	{
		"Rational{Int64}",
		"Rational64",
		nil,
		"def output = rational[64, 1, 2]",
		[]interface{}{[]interface{}{int64(1), int64(2)}},
		[]interface{}{big.NewRat(1, 2)},
		false,
	},
	// FIXME: big.NewRat don't support big.Int as value
	{
		"Rational{Int128}",
		"Rational128",
		nil,
		"def output = rational[128, 123456789101112313, 9123456789101112313]",
		//[]interface{}{[]interface{}{uint64(123456789101112313), uint64(0)}, []interface{}{uint64(9123456789101112313), uint64(0)}},
		[]interface{}{[]interface{}{[]interface{}{uint64(123456789101112313), uint64(0), uint64(9123456789101112313), uint64(0)}}},
		[]interface{}{big.NewRat(strToBig("9123456789101112313").Int64(), strToBig("123456789101112313").Int64())},
		true,
	},
}
