// Copyright 2022 RelationalAI, Inc.

package rai

import (
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v7/arrow/float16"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

// track test results in transactions db
const o11yTag = "rai-sdk-go:test"

type tdata struct {
	sig  Signature
	cols [][]any
}

type execTest struct {
	query string
	mdata map[string]Signature // expected metadata
	pdata map[string]tdata     // expected partition data
	rdata map[string]tdata     // expected relation data
}

func asAny[T any](args ...T) []any {
	result := make([]any, len(args))
	for i, arg := range args {
		result[i] = arg
	}
	return result
}

func value(args ...any) []any {
	return args
}

// Returns a row of data, in columnar form, constructed from the givne args.
func row(args ...any) [][]any {
	result := make([][]any, len(args))
	for i, arg := range args {
		result[i] = []any{arg}
	}
	return result
}

// Returns a metadata mapping constructed from the given args.
func mdata(args ...any) map[string]Signature {
	n := len(args)
	result := map[string]Signature{}
	for i := 0; i < n; i += 2 {
		id := args[i].(string)
		sig := args[i+1].(Signature)
		result[id] = sig
	}
	return result
}

// Returns a signature constructed from the given args.
func sig(args ...any) Signature {
	if len(args) == 0 {
		return Signature{}
	}
	return args
}

// Returns a ValueType signature constructed from the given args.
func vtype(args ...any) ValueType {
	vt := ValueType{}
	if name, ok := args[0].(string); ok {
		vt, args = asAny(strings.Split(name, ":")...), args[1:]
	}
	vt = append(vt, args...)
	return ValueType(vt)
}

// Returns a ConstType signature constructed from the given args. Note, there
// are currently many cases where a ConstType loses its identifier, so we
// handle that here conditionally until the engine bugs are fixed.
func ctype(args ...any) ConstType {
	ct := ConstType{}
	if len(args) == 0 {
		return ct
	}
	if name, ok := args[0].(string); ok {
		ct, args = asAny(strings.Split(name, ":")...), args[1:]
	}
	return append(ct, args...)
}

// Construct a map of expected tdata (tabular data) values.
func xdata(args ...any) map[string]tdata {
	n := len(args)
	result := map[string]tdata{}
	for i := 0; i < n; i += 3 {
		id := args[i].(string)
		arg2 := args[i+1]
		arg3 := args[i+2]
		var sig Signature
		if arg2 == nil {
			sig = nil
		} else {
			sig = arg2.(Signature)
		}
		var cols [][]any
		if arg3 == nil {
			cols = nil
		} else {
			cols = arg3.([][]any)
		}
		result[id] = tdata{sig, cols}
	}
	return result
}

var primitiveTypeTests = []execTest{
	{
		query: `def output = "test"`,
		mdata: mdata("0.arrow", sig("output", StringType)),
		pdata: xdata("0.arrow", sig(StringType), row("test")),
		rdata: xdata("0.arrow", sig("output", StringType), row("output", "test")),
	},
	{
		query: `def output = boolean_true`,
		mdata: mdata("0.arrow", sig("output", BoolType)),
		pdata: xdata("0.arrow", sig(BoolType), row(true)),
		rdata: xdata("0.arrow", sig("output", BoolType), row("output", true)),
	},
	{
		query: `def output = boolean_false`,
		mdata: mdata("0.arrow", sig("output", BoolType)),
		pdata: xdata("0.arrow", sig(BoolType), row(false)),
		rdata: xdata("0.arrow", sig("output", BoolType), row("output", false)),
	},
	{
		query: `def output = 'a', '👍'`,
		mdata: mdata("0.arrow", sig("output", CharType, CharType)),
		pdata: xdata("0.arrow", sig(Uint32Type, Uint32Type), row(uint32(97), uint32(128077))),
		rdata: xdata("0.arrow", sig("output", RuneType, RuneType), row("output", 'a', '👍')),
	},
	{
		query: `def output = 2021-10-12T01:22:31+10:00`,
		mdata: mdata("0.arrow", sig("output", vtype("rel:base:DateTime", Int64Type))),
		pdata: xdata("0.arrow", sig(Int64Type), row(int64(63769648951000))),
		rdata: xdata("0.arrow", sig("output", TimeType),
			row("output", DateFromRataMillis(63769648951000))),
	},
	{
		query: `def output = 2021-10-12`,
		mdata: mdata("0.arrow", sig("output", vtype("rel:base:Date", Int64Type))),
		pdata: xdata("0.arrow", sig(Int64Type), row(int64(738075))),
		rdata: xdata("0.arrow", sig("output", TimeType),
			row("output", DateFromRataDie(738075))),
	},
	{
		query: `def output = Year[2022]`,
		mdata: mdata("0.arrow", sig("output", vtype("rel:base:Year", Int64Type))),
		pdata: xdata("0.arrow", sig(Int64Type), row(int64(2022))),
		rdata: xdata("0.arrow", sig("output", Int64Type), row("output", int64(2022))),
	},
	{
		query: `def output = Month[1]`,
		mdata: mdata("0.arrow", sig("output", vtype("rel:base:Month", Int64Type))),
		pdata: xdata("0.arrow", sig(Int64Type), row(int64(1))),
		rdata: xdata("0.arrow", sig("output", Int64Type), row("output", int64(1))),
	},
	{
		query: `def output = Week[1]`,
		mdata: mdata("0.arrow", sig("output", vtype("rel:base:Week", Int64Type))),
		pdata: xdata("0.arrow", sig(Int64Type), row(int64(1))),
		rdata: xdata("0.arrow", sig("output", Int64Type), row("output", int64(1))),
	},
	{
		query: `def output = Day[1]`,
		mdata: mdata("0.arrow", sig("output", vtype("rel:base:Day", Int64Type))),
		pdata: xdata("0.arrow", sig(Int64Type), row(int64(1))),
		rdata: xdata("0.arrow", sig("output", Int64Type), row("output", int64(1))),
	},
	{
		query: `def output = Hour[1]`,
		mdata: mdata("0.arrow", sig("output", vtype("rel:base:Hour", Int64Type))),
		pdata: xdata("0.arrow", sig(Int64Type), row(int64(1))),
		rdata: xdata("0.arrow", sig("output", Int64Type), row("output", int64(1))),
	},
	{
		query: `def output = Minute[1]`,
		mdata: mdata("0.arrow", sig("output", vtype("rel:base:Minute", Int64Type))),
		pdata: xdata("0.arrow", sig(Int64Type), row(int64(1))),
		rdata: xdata("0.arrow", sig("output", Int64Type), row("output", int64(1))),
	},
	{
		query: `def output = Second[1]`,
		mdata: mdata("0.arrow", sig("output", vtype("rel:base:Second", Int64Type))),
		pdata: xdata("0.arrow", sig(Int64Type), row(int64(1))),
		rdata: xdata("0.arrow", sig("output", Int64Type), row("output", int64(1))),
	},
	{
		query: `def output = Millisecond[1]`,
		mdata: mdata("0.arrow", sig("output", vtype("rel:base:Millisecond", Int64Type))),
		pdata: xdata("0.arrow", sig(Int64Type), row(int64(1))),
		rdata: xdata("0.arrow", sig("output", Int64Type), row("output", int64(1))),
	},
	{
		query: `def output = Microsecond[1]`,
		mdata: mdata("0.arrow", sig("output", vtype("rel:base:Microsecond", Int64Type))),
		pdata: xdata("0.arrow", sig(Int64Type), row(int64(1))),
		rdata: xdata("0.arrow", sig("output", Int64Type), row("output", int64(1))),
	},
	{
		query: `def output = Nanosecond[1]`,
		mdata: mdata("0.arrow", sig("output", vtype("rel:base:Nanosecond", Int64Type))),
		pdata: xdata("0.arrow", sig(Int64Type), row(int64(1))),
		rdata: xdata("0.arrow", sig("output", Int64Type), row("output", int64(1))),
	},
	{
		query: `
			entity type Foo = Int
			def output = ^Foo[12]`,
		mdata: mdata("0.arrow", sig("output", vtype("rel:base:Hash", Uint128Type))),
		pdata: xdata("0.arrow", sig(Uint64ListType),
			row([]uint64{uint64(10589367010498591262), uint64(15771123988529185405)})),
		rdata: xdata("0.arrow", sig("output", BigIntType),
			row("output", NewBigUint128(10589367010498591262, 15771123988529185405))),
	},
	{
		query: `def output = missing`,
		mdata: mdata("0.arrow", sig("output", vtype("rel:base:Missing"))),
		pdata: xdata("0.arrow", sig(StructType), [][]any{{}}),
		rdata: xdata("0.arrow", sig("output", MissingType), row("output", "missing")),
	},
	{
		query: `
			def config:data="""
				a,b,c
				1,2,3
				4,5,6"""
			def csv = load_csv[config]
			def output(p) = csv(_, p, _)`,
		mdata: mdata("0.arrow", sig("output", vtype("rel:base:FilePos", Int64Type))),
		pdata: xdata("0.arrow", sig(Int64Type), [][]any{{int64(2), int64(3)}}),
		rdata: xdata("0.arrow", sig("output", Int64Type),
			[][]any{{"output", "output"}, {int64(2), int64(3)}}),
	},
	{
		query: `def output = int[8, 12], int[8, -12]`,
		mdata: mdata("0.arrow", sig("output", Int8Type, Int8Type)),
		pdata: xdata("0.arrow", sig(Int8Type, Int8Type), row(int8(12), int8(-12))),
		rdata: xdata("0.arrow", sig("output", Int8Type, Int8Type),
			row("output", int8(12), int8(-12))),
	},
	{
		query: `def output = int[16, 123], int[16, -123]`,
		mdata: mdata("0.arrow", sig("output", Int16Type, Int16Type)),
		pdata: xdata("0.arrow", sig(Int16Type, Int16Type),
			row(int16(123), int16(-123))),
		rdata: xdata("0.arrow", sig("output", Int16Type, Int16Type),
			row("output", int16(123), int16(-123))),
	},
	{
		query: `def output = int[32, 1234], int[32, -1234]`,
		mdata: mdata("0.arrow", sig("output", Int32Type, Int32Type)),
		pdata: xdata("0.arrow", sig(Int32Type, Int32Type),
			row(int32(1234), int32(-1234))),
		rdata: xdata("0.arrow", sig("output", Int32Type, Int32Type),
			row("output", int32(1234), int32(-1234))),
	},
	{
		query: `def output = 12345, -12345`,
		mdata: mdata("0.arrow", sig("output", Int64Type, Int64Type)),
		pdata: xdata("0.arrow", sig(Int64Type, Int64Type),
			row(int64(12345), int64(-12345))),
		rdata: xdata("0.arrow", sig("output", Int64Type, Int64Type),
			row("output", int64(12345), int64(-12345))),
	},
	{
		query: `def output = 123456789101112131415, int[128, 0], int[128, -10^10]`,
		mdata: mdata("0.arrow", sig("output", Int128Type, Int128Type, Int128Type)),
		pdata: xdata("0.arrow", sig(Uint64ListType, Uint64ListType, Uint64ListType),
			row([]uint64{uint64(12776324658854821719), uint64(6)},
				[]uint64{uint64(0), uint64(0)},
				[]uint64{uint64(18446744063709551616), uint64(18446744073709551615)})),
		rdata: xdata("0.arrow", sig("output", BigIntType, BigIntType, BigIntType),
			row("output",
				NewBigInt128(12776324658854821719, 6), NewBigInt128(0, 0),
				NewBigInt128(18446744063709551616, 18446744073709551615))),
	},
	{
		query: `def output = uint[8, 12]`,
		mdata: mdata("0.arrow", sig("output", Uint8Type)),
		pdata: xdata("0.arrow", sig(Uint8Type), row(uint8(12))),
		rdata: xdata("0.arrow", sig("output", Uint8Type), row("output", uint8(12))),
	},
	{
		query: `def output = uint[16, 123]`,
		mdata: mdata("0.arrow", sig("output", Uint16Type)),
		pdata: xdata("0.arrow", sig(Uint16Type), row(uint16(123))),
		rdata: xdata("0.arrow", sig("output", Uint16Type), row("output", uint16(123))),
	},
	{
		query: `def output = uint[32, 1234]`,
		mdata: mdata("0.arrow", sig("output", Uint32Type)),
		pdata: xdata("0.arrow", sig(Uint32Type), row(uint32(1234))),
		rdata: xdata("0.arrow", sig("output", Uint32Type), row("output", uint32(1234))),
	},
	{
		query: `def output = uint[64, 12345]`,
		mdata: mdata("0.arrow", sig("output", Uint64Type)),
		pdata: xdata("0.arrow", sig(Uint64Type), row(uint64(12345))),
		rdata: xdata("0.arrow", sig("output", Uint64Type), row("output", uint64(12345))),
	},
	{
		query: `def output = uint[128, 123456789101112131415], uint[128, 0], 0xdade49b564ec827d92f4fd30f1023a1e`,
		mdata: mdata("0.arrow", sig("output", Uint128Type, Uint128Type, Uint128Type)),
		pdata: xdata("0.arrow", sig(Uint64ListType, Uint64ListType, Uint64ListType),
			row([]uint64{12776324658854821719, 6}, []uint64{0, 0},
				[]uint64{10589367010498591262, 15771123988529185405})),
		rdata: xdata("0.arrow",
			sig("output", BigIntType, BigIntType, BigIntType),
			row("output", NewBigUint128(12776324658854821719, 6), NewBigUint128(0, 0),
				NewBigUint128(10589367010498591262, 15771123988529185405))),
	},
	{
		query: `def output = float[16, 12], float[16, 42.5]`,
		mdata: mdata("0.arrow", sig("output", Float16Type, Float16Type)),
		pdata: xdata("0.arrow", sig(Float16Type, Float16Type),
			row(float16.New(12), float16.New(42.5))),
		rdata: xdata("0.arrow", sig("output", Float16Type, Float16Type),
			row("output", float16.New(12), float16.New(42.5))),
	},
	{
		query: `def output = float[32, 12], float[32, 42.5]`,
		mdata: mdata("0.arrow", sig("output", Float32Type, Float32Type)),
		pdata: xdata("0.arrow", sig(Float32Type, Float32Type),
			row(float32(12), float32(42.5))),
		rdata: xdata("0.arrow", sig("output", Float32Type, Float32Type),
			row("output", float32(12), float32(42.5))),
	},
	{
		query: `def output = float[64, 12], float[64, 42.5]`,
		mdata: mdata("0.arrow", sig("output", Float64Type, Float64Type)),
		pdata: xdata("0.arrow", sig(Float64Type, Float64Type),
			row(float64(12), float64(42.5))),
		rdata: xdata("0.arrow", sig("output", Float64Type, Float64Type),
			row("output", float64(12), float64(42.5))),
	},
	{
		query: `def output = parse_decimal[16, 2, "12.34"]`,
		mdata: mdata("0.arrow",
			sig("output", vtype("rel:base:FixedDecimal",
				int64(16), int64(2), Int16Type))),
		pdata: xdata("0.arrow", sig(Int16Type), row(int16(1234))),
		rdata: xdata("0.arrow", sig("output", DecimalType),
			row("output", decimal.New(1234, -2))),
	},
	{
		query: `def output = parse_decimal[32, 2, "12.34"]`,
		mdata: mdata("0.arrow", sig("output", vtype("rel:base:FixedDecimal",
			int64(32), int64(2), Int32Type))),
		pdata: xdata("0.arrow", sig(Int32Type), row(int32(1234))),
		rdata: xdata("0.arrow", sig("output", DecimalType),
			row("output", decimal.New(1234, -2))),
	},
	{
		query: `def output = parse_decimal[64, 2, "12.34"]`,
		mdata: mdata("0.arrow",
			sig("output", vtype("rel:base:FixedDecimal",
				int64(64), int64(2), Int64Type))),
		pdata: xdata("0.arrow", sig(Int64Type), row(int64(1234))),
		rdata: xdata("0.arrow", sig("output", DecimalType),
			row("output", decimal.New(1234, -2))),
	},
	{
		query: `def output = parse_decimal[128, 2, "12345678901011121314.34"]`,
		mdata: mdata("0.arrow", sig("output", vtype("rel:base:FixedDecimal",
			int64(128), int64(2), Int128Type))),
		pdata: xdata("0.arrow",
			sig(Uint64ListType), row([]uint64{17082781236281724778, 66})),
		rdata: xdata("0.arrow", sig("output", DecimalType),
			row("output", NewDecimal128(17082781236281724778, 66, -2))),
	},
	{
		query: `def output = rational[8, 1, 2]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("rel:base:Rational", int64(8), Int8Type, Int8Type))),
		pdata: xdata("0.arrow", sig(Int8ListType), row([]int8{int8(1), int8(2)})),
		rdata: xdata("0.arrow", sig("output", RationalType), row("output", big.NewRat(1, 2))),
	},
	{
		query: `def output = rational[16, 1, 2]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("rel:base:Rational", int64(16), Int16Type, Int16Type))),
		pdata: xdata("0.arrow", sig(Int16ListType), row([]int16{int16(1), int16(2)})),
		rdata: xdata("0.arrow", sig("output", RationalType), row("output", big.NewRat(1, 2))),
	},
	{
		query: `def output = rational[32, 1, 2]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("rel:base:Rational", int64(32), Int32Type, Int32Type))),
		pdata: xdata("0.arrow", sig(Int32ListType), row([]int32{int32(1), int32(2)})),
		rdata: xdata("0.arrow", sig("output", RationalType), row("output", big.NewRat(1, 2))),
	},
	{
		query: `def output = rational[64, 1, 2]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("rel:base:Rational", int64(64), Int64Type, Int64Type))),
		pdata: xdata("0.arrow", sig(Int64ListType), row([]int64{int64(1), int64(2)})),
		rdata: xdata("0.arrow", sig("output", RationalType), row("output", big.NewRat(1, 2))),
	},
	{
		query: `def output = rational[128, 123456789101112313, 9123456789101112313]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("rel:base:Rational", int64(128), Int128Type, Int128Type))),
		pdata: xdata("0.arrow", sig(Uint64ListType),
			row([]uint64{123456789101112313, 0, 9123456789101112313, 0})),
		rdata: xdata("0.arrow", sig("output", RationalType),
			row("output", NewRational128(
				NewBigInt128(123456789101112313, 0),
				NewBigInt128(9123456789101112313, 0)))),
	},
}

var constPrimitiveTypeTests = []execTest{
	{
		query: `def output = :foo`,
		mdata: mdata("0.arrow", sig("output", "foo")),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", "foo"), row("output", "foo")),
	},
	{
		query: `def output = #("foo")`,
		mdata: mdata("0.arrow", sig("output", "foo")),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", "foo"), row("output", "foo")),
	},
	{
		query: `def output = #("foo / bar")`,
		mdata: mdata("0.arrow", sig("output", "foo / bar")),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", "foo / bar"), row("output", "foo / bar")),
	},
	{
		query: `def output = #(boolean_true)`,
		mdata: mdata("0.arrow", sig("output", true)),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", true), row("output", true)),
	},
	{
		query: `def output = #(boolean_false)`,
		mdata: mdata("0.arrow", sig("output", false)),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", false), row("output", false)),
	},
	{
		query: `def output = #('👍')`,
		mdata: mdata("0.arrow", sig("output", '👍')),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", '👍'), row("output", '👍')),
	},
	{
		query: `def output = #(2021-10-12T01:22:31+10:00)`,
		mdata: mdata("0.arrow", sig("output",
			ctype("rel:base:DateTime", int64(63801184951000)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", DateFromRataMillis(63801184951000)),
			row("output", DateFromRataMillis(63801184951000))),
	},
	{
		query: `def output = #(2021-10-12)`,
		mdata: mdata("0.arrow", sig("output",
			ctype("rel:base:Date", int64(738075)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", DateFromRataDie(738075)),
			row("output", DateFromRataDie(738075))),
	},
	{
		query: `def output = #(Year[2022])`,
		mdata: mdata("0.arrow", sig("output", ctype("rel:base:Year", int64(2022)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", int64(2022)), row("output", int64(2022))),
	},
	{
		query: `def output = #(Month[1])`,
		mdata: mdata("0.arrow", sig("output", ctype("rel:base:Month", int64(1)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", int64(1)), row("output", int64(1))),
	},
	{
		query: `def output = #(Week[1])`,
		mdata: mdata("0.arrow", sig("output", ctype("rel:base:Week", int64(1)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", int64(1)), row("output", int64(1))),
	},
	{
		query: `def output = #(Day[1])`,
		mdata: mdata("0.arrow", sig("output", ctype("rel:base:Day", int64(1)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", int64(1)), row("output", int64(1))),
	},
	{
		query: `def output = #(Hour[1])`,
		mdata: mdata("0.arrow", sig("output", ctype("rel:base:Hour", int64(1)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", int64(1)), row("output", int64(1))),
	},
	{
		query: `def output = #(Minute[1])`,
		mdata: mdata("0.arrow", sig("output", ctype("rel:base:Minute", int64(1)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", int64(1)), row("output", int64(1))),
	},
	{
		query: `def output = #(Second[1])`,
		mdata: mdata("0.arrow", sig("output", ctype("rel:base:Second", int64(1)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", int64(1)), row("output", int64(1))),
	},
	{
		query: `def output = #(Millisecond[1])`,
		mdata: mdata("0.arrow", sig("output", ctype("rel:base:Millisecond", int64(1)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", int64(1)), row("output", int64(1))),
	},
	{
		query: `def output = #(Microsecond[1])`,
		mdata: mdata("0.arrow", sig("output", ctype("rel:base:Microsecond", int64(1)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", int64(1)), row("output", int64(1))),
	},
	{
		query: `def output = #(Nanosecond[1])`,
		mdata: mdata("0.arrow", sig("output", ctype("rel:base:Nanosecond", int64(1)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", int64(1)), row("output", int64(1))),
	},
	{
		query: `
			entity type Foo = Int
			def foo = ^Foo[12]
			def output = #(foo)`,
		mdata: mdata("0.arrow",
			sig("output", ctype("rel:base:Hash",
				NewBigUint128(10589367010498591262, 15771123988529185405)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", NewBigUint128(10589367010498591262, 15771123988529185405)),
			row("output", NewBigUint128(10589367010498591262, 15771123988529185405))),
	},
	// {query: `def output = #(missing)`},
	{
		query: `
			def config:data="""
			a,b,c
			1,2,3"""
			def csv = load_csv[config]
			def v(p) = csv(_, p, _)
			def output = #(v)`,
		mdata: mdata("0.arrow", sig("output", ctype("rel:base:FilePos", int64(2)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", int64(2)), row("output", int64(2))),
	},
	{
		query: `def output = #(int[8, -12])`,
		mdata: mdata("0.arrow", sig("output", int8(-12))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", int8(-12)), row("output", int8(-12))),
	},
	{
		query: `def output = #(int[16, -123]) `,
		mdata: mdata("0.arrow", sig("output", int16(-123))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", int16(-123)), row("output", int16(-123))),
	},
	{
		query: `def output = #(int[32, -1234])`,
		mdata: mdata("0.arrow", sig("output", int32(-1234))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", int32(-1234)), row("output", int32(-1234))),
	},
	{
		query: `def output = #(int[64, -12345])`,
		mdata: mdata("0.arrow", sig("output", int64(-12345))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", int64(-12345)), row("output", int64(-12345))),
	},
	{
		query: `def output = #(int[128, 123456789101112131415])`,
		mdata: mdata("0.arrow", sig("output", NewBigInt128(12776324658854821719, 6))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", NewBigInt128(12776324658854821719, 6)),
			row("output", NewBigInt128(12776324658854821719, 6))),
	},
	{
		query: `def output = #(uint[8, 12])`,
		mdata: mdata("0.arrow", sig("output", uint8(12))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", uint8(12)), row("output", uint8(12))),
	},
	{
		query: `def output = #(uint[16, 123])`,
		mdata: mdata("0.arrow", sig("output", uint16(123))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", uint16(123)), row("output", uint16(123))),
	},
	{
		query: `def output = #(uint[32, 1234])`,
		mdata: mdata("0.arrow", sig("output", uint32(1234))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", uint32(1234)), row("output", uint32(1234))),
	},
	{
		query: `def output = #(uint[64, 12345])`,
		mdata: mdata("0.arrow", sig("output", uint64(12345))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", uint64(12345)), row("output", uint64(12345))),
	},
	{
		query: `def output = #(uint[128, 123456789101112131415])`,
		mdata: mdata("0.arrow", sig("output", NewBigUint128(12776324658854821719, 6))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", NewBigUint128(12776324658854821719, 6)),
			row("output", NewBigUint128(12776324658854821719, 6))),
	},
	{
		query: `def output = #(float[16, 42.5])`,
		mdata: mdata("0.arrow", sig("output", float16.New(42.5))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", float16.New(42.5)),
			row("output", float16.New(42.5))),
	},
	{
		query: `def output = #(float[32, 42.5])`,
		mdata: mdata("0.arrow", sig("output", float32(42.5))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", float32(42.5)),
			row("output", float32(42.5))),
	},
	{
		query: `def output = #(float[64, 42.5])`,
		mdata: mdata("0.arrow", sig("output", float64(42.5))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", float64(42.5)),
			row("output", float64(42.5))),
	},
	{
		query: `def output = #(parse_decimal[16, 2, "12.34"])`,
		mdata: mdata("0.arrow",
			sig("output", ctype("rel:base:FixedDecimal", int64(16), int64(2), int16(1234)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", decimal.New(1234, -2)),
			row("output", decimal.New(1234, -2))),
	},
	{
		query: `def output = #(parse_decimal[32, 2, "12.34"])`,
		mdata: mdata("0.arrow", sig("output",
			ctype("rel:base:FixedDecimal", int64(32), int64(2), int32(1234)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", decimal.New(1234, -2)),
			row("output", decimal.New(1234, -2))),
	},
	{
		query: `def output = #(parse_decimal[64, 2, "12.34"])`,
		mdata: mdata("0.arrow", sig("output",
			ctype("rel:base:FixedDecimal", int64(64), int64(2), int64(1234)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", decimal.New(1234, -2)),
			row("output", decimal.New(1234, -2))),
	},
	{
		query: `def output = #(parse_decimal[128, 2, "12345678901011121314.34"])`,
		mdata: mdata("0.arrow", sig("output",
			ctype("rel:base:FixedDecimal", int64(128), int64(2),
				NewBigInt128(17082781236281724778, 66)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", NewDecimal128(17082781236281724778, 66, -2)),
			row("output", NewDecimal128(17082781236281724778, 66, -2))),
	},
	{
		query: `def output = #(rational[8, 1, 2])`,
		mdata: mdata("0.arrow", sig("output",
			ctype("rel:base:Rational", int64(8), int8(1), int8(2)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", big.NewRat(1, 2)),
			row("output", big.NewRat(1, 2))),
	},
	{
		query: `def output = #(rational[16, 1, 2])`,
		mdata: mdata("0.arrow", sig("output",
			ctype("rel:base:Rational", int64(16), int16(1), int16(2)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", big.NewRat(1, 2)),
			row("output", big.NewRat(1, 2))),
	},
	{
		query: `def output = #(rational[32, 1, 2])`,
		mdata: mdata("0.arrow", sig("output",
			ctype("rel:base:Rational", int64(32), int32(1), int32(2)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", big.NewRat(1, 2)),
			row("output", big.NewRat(1, 2))),
	},
	{
		query: `def output = #(rational[64, 1, 2])`,
		mdata: mdata("0.arrow", sig("output",
			ctype("rel:base:Rational", int64(64), int64(1), int64(2)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow", sig("output", big.NewRat(1, 2)),
			row("output", big.NewRat(1, 2))),
	},
	{
		query: `def output = #(rational[128, 123456789101112313, 9123456789101112313])`,
		mdata: mdata("0.arrow", sig("output",
			ctype("rel:base:Rational", int64(128),
				NewBigInt128(123456789101112313, 0),
				NewBigInt128(9123456789101112313, 0)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", NewRational128(
				NewBigInt128(123456789101112313, 0),
				NewBigInt128(9123456789101112313, 0))),
			row("output", NewRational128(
				NewBigInt128(123456789101112313, 0),
				NewBigInt128(9123456789101112313, 0)))),
	},
}

var valueTypeTests = []execTest{
	{
		query: `
			value type MyType = :foo; :bar; :baz
			def output = ^MyType[:foo]`,
		mdata: mdata("0.arrow", sig("output", vtype("MyType", "foo"))),
		pdata: xdata("0.arrow", sig(StructType), [][]any{{}}),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", "foo")),
			row("output", value("MyType", "foo"))),
	},
	{
		query: `
			value type MyType = Int, String
			def output = ^MyType[1, "abc"]`,
		mdata: mdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, StringType))),
		pdata: xdata("0.arrow", sig(StructType), row([]any{int64(1), "abc"})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, StringType)),
			row("output", value("MyType", int64(1), "abc"))),
	},
	{
		query: `
			value type MyType = Int, Boolean
			def output = ^MyType[1, boolean_true]`,
		mdata: mdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, BoolType))),
		pdata: xdata("0.arrow", sig(StructType), row([]any{int64(1), true})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, BoolType)),
			row("output", value("MyType", int64(1), true))),
	},
	{
		query: `
			value type MyType = Int, Boolean
			def output = ^MyType[1, boolean_false]`,
		mdata: mdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, BoolType))),
		pdata: xdata("0.arrow", sig(StructType), row([]any{int64(1), false})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, BoolType)),
			row("output", value("MyType", int64(1), false))),
	},
	{
		query: `
			value type MyType = Int, Char
			def output = ^MyType[1, '👍']`,
		mdata: mdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, CharType))),
		pdata: xdata("0.arrow", sig(StructType),
			row([]any{int64(1), uint32('👍')})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, RuneType)),
			row("output", value("MyType", int64(1), '👍'))),
	},
	{
		query: `
			value type MyType = Int, DateTime
			def output = ^MyType[1, 2021-10-12T01:22:31+10:00]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("MyType", Int64Type, vtype("rel:base:DateTime", Int64Type)))),
		pdata: xdata("0.arrow", sig(Int64ListType),
			row([]int64{1, 63769648951000})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, TimeType)),
			row("output", value("MyType", int64(1),
				DateFromRataMillis(63769648951000)))),
	},
	{
		query: `
			value type MyType = Int, Date
			def output = ^MyType[1, 2021-10-12]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("MyType", Int64Type, vtype("rel:base:Date", Int64Type)))),
		pdata: xdata("0.arrow", sig(Int64ListType), row([]int64{1, 738075})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, TimeType)),
			row("output", value("MyType", int64(1), DateFromRataDie(738075)))),
	},
	{
		query: `
			value type MyType = Int, is_Year
			def output = ^MyType[1, Year[2022]]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("MyType", Int64Type, vtype("rel:base:Year", Int64Type)))),
		pdata: xdata("0.arrow", sig(Int64ListType), row([]int64{1, 2022})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, Int64Type)),
			row("output", value("MyType", int64(1), int64(2022)))),
	},
	{
		query: `
			value type MyType = Int, is_Month
			def output = ^MyType[1, Month[2]]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("MyType", Int64Type, vtype("rel:base:Month", Int64Type)))),
		pdata: xdata("0.arrow", sig(Int64ListType), row([]int64{1, 2})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, Int64Type)),
			row("output", value("MyType", int64(1), int64(2)))),
	},
	{
		query: `
			value type MyType = Int, is_Week
			def output = ^MyType[1, Week[2]]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("MyType", Int64Type, vtype("rel:base:Week", Int64Type)))),
		pdata: xdata("0.arrow", sig(Int64ListType), row([]int64{1, 2})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, Int64Type)),
			row("output", value("MyType", int64(1), int64(2)))),
	},
	{
		query: `
			value type MyType = Int, is_Day
			def output = ^MyType[1, Day[2]]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("MyType", Int64Type, vtype("rel:base:Day", Int64Type)))),
		pdata: xdata("0.arrow", sig(Int64ListType), row([]int64{1, 2})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, Int64Type)),
			row("output", value("MyType", int64(1), int64(2)))),
	},
	{
		query: `
			value type MyType = Int, is_Hour
			def output = ^MyType[1, Hour[2]]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("MyType", Int64Type, vtype("rel:base:Hour", Int64Type)))),
		pdata: xdata("0.arrow", sig(Int64ListType), row([]int64{1, 2})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, Int64Type)),
			row("output", value("MyType", int64(1), int64(2)))),
	},
	{
		query: `
			value type MyType = Int, is_Minute
			def output = ^MyType[1, Minute[2]]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("MyType", Int64Type, vtype("rel:base:Minute", Int64Type)))),
		pdata: xdata("0.arrow", sig(Int64ListType), row([]int64{1, 2})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, Int64Type)),
			row("output", value("MyType", int64(1), int64(2)))),
	},
	{
		query: `
			value type MyType = Int, is_Second
			def output = ^MyType[1, Second[2]]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("MyType", Int64Type, vtype("rel:base:Second", Int64Type)))),
		pdata: xdata("0.arrow", sig(Int64ListType), row([]int64{1, 2})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, Int64Type)),
			row("output", value("MyType", int64(1), int64(2)))),
	},
	{
		query: `
			value type MyType = Int, is_Millisecond
			def output = ^MyType[1, Millisecond[2]]`,
		mdata: mdata("0.arrow", sig("output", vtype("MyType", Int64Type,
			vtype("rel:base:Millisecond", Int64Type)))),
		pdata: xdata("0.arrow", sig(Int64ListType), row([]int64{1, 2})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, Int64Type)),
			row("output", value("MyType", int64(1), int64(2)))),
	},
	{
		query: `
			value type MyType = Int, is_Microsecond
			def output = ^MyType[1, Microsecond[2]]`,
		mdata: mdata("0.arrow", sig("output", vtype("MyType", Int64Type,
			vtype("rel:base:Microsecond", Int64Type)))),
		pdata: xdata("0.arrow", sig(Int64ListType), row([]int64{1, 2})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, Int64Type)),
			row("output", value("MyType", int64(1), int64(2)))),
	},
	{
		query: `
			value type MyType = Int, is_Nanosecond
			def output = ^MyType[1, Nanosecond[2]]`,
		mdata: mdata("0.arrow", sig("output", vtype("MyType", Int64Type,
			vtype("rel:base:Nanosecond", Int64Type)))),
		pdata: xdata("0.arrow", sig(Int64ListType), row([]int64{1, 2})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, Int64Type)),
			row("output", value("MyType", int64(1), int64(2)))),
	},
	{
		query: `
			value type MyType = Int, Hash
			def h(x) = hash128["abc", _, x]
			def output = ^MyType[1, h]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("MyType", Int64Type, vtype("rel:base:Hash", Uint128Type)))),
		pdata: xdata("0.arrow", sig(StructType),
			row([]any{int64(1),
				[]uint64{3877405323480549948, 3198683864092244389}})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, BigIntType)),
			row("output", value("MyType", int64(1),
				NewBigUint128(3877405323480549948, 3198683864092244389)))),
	},
	{
		query: `
			value type MyType = Int, Missing
			def output = ^MyType[1, missing]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("MyType", Int64Type, vtype("rel:base:Missing")))),
		pdata: xdata("0.arrow", sig(StructType), row([]any{int64(1), []any{}})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, MissingType)),
			row("output", value("MyType", int64(1), "missing"))),
	},
	{
		query: `
			def config:data="""
				a,b,c
				1,2,3"""
			def csv = load_csv[config]
			def v(p) = csv(_, p, _)
			value type MyType = Int, FilePos
			def output = ^MyType[1, v]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("MyType", Int64Type, vtype("rel:base:FilePos", Int64Type)))),
		pdata: xdata("0.arrow", sig(Int64ListType), row([]int64{1, 2})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, Int64Type)),
			row("output", value("MyType", int64(1), int64(2)))),
	},
	{
		query: `
			value type MyType = Int, SignedInt[8]
			def output = ^MyType[1, int[8, -12]]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("MyType", Int64Type, Int8Type))),
		pdata: xdata("0.arrow", sig(StructType),
			row([]any{int64(1), int8(-12)})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, Int8Type)),
			row("output", value("MyType", int64(1), int8(-12)))),
	},
	{
		query: `
			value type MyType = Int, SignedInt[16]
			def output = ^MyType[1, int[16, -123]]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("MyType", Int64Type, Int16Type))),
		pdata: xdata("0.arrow", sig(StructType),
			row([]any{int64(1), int16(-123)})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, Int16Type)),
			row("output", value("MyType", int64(1), int16(-123)))),
	},
	{
		query: `
			value type MyType = Int, SignedInt[32]
			def output = ^MyType[1, int[32, -1234]]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("MyType", Int64Type, Int32Type))),
		pdata: xdata("0.arrow", sig(StructType),
			row([]any{int64(1), int32(-1234)})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, Int32Type)),
			row("output", value("MyType", int64(1), int32(-1234)))),
	},
	{
		query: `
			value type MyType = Int, SignedInt[64]
			def output = ^MyType[1, int[64, -12345]]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("MyType", Int64Type, Int64Type))),
		pdata: xdata("0.arrow", sig(Int64ListType), row([]int64{1, -12345})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, Int64Type)),
			row("output", value("MyType", int64(1), int64(-12345)))),
	},
	{
		query: `
			value type MyType = Int, SignedInt[128]
			def output = ^MyType[1, int[128, 123456789101112131415]]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("MyType", Int64Type, Int128Type))),
		pdata: xdata("0.arrow", sig(StructType),
			row([]any{int64(1), []uint64{12776324658854821719, 6}})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, BigIntType)),
			row("output", value("MyType", int64(1), NewBigInt128(12776324658854821719, 6)))),
	},
	{
		query: `
			value type MyType = Int, UnsignedInt[8]
			def output = ^MyType[1, uint[8, 12]]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("MyType", Int64Type, Uint8Type))),
		pdata: xdata("0.arrow", sig(StructType),
			row([]any{int64(1), uint8(12)})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, Uint8Type)),
			row("output", value("MyType", int64(1), uint8(12)))),
	},
	{
		query: `
			value type MyType = Int, UnsignedInt[16]
			def output = ^MyType[1, uint[16, 123]]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("MyType", Int64Type, Uint16Type))),
		pdata: xdata("0.arrow", sig(StructType),
			row([]any{int64(1), uint16(123)})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, Uint16Type)),
			row("output", value("MyType", int64(1), uint16(123)))),
	},
	{
		query: `
			value type MyType = Int, UnsignedInt[32]
			def output = ^MyType[1, uint[32, 1234]]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("MyType", Int64Type, Uint32Type))),
		pdata: xdata("0.arrow", sig(StructType),
			row([]any{int64(1), uint32(1234)})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, Uint32Type)),
			row("output", value("MyType", int64(1), uint32(1234)))),
	},
	{
		query: `
			value type MyType = Int, UnsignedInt[64]
			def output = ^MyType[1, uint[64, 12345]]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("MyType", Int64Type, Uint64Type))),
		pdata: xdata("0.arrow", sig(StructType),
			row([]any{int64(1), uint64(12345)})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, Uint64Type)),
			row("output", value("MyType", int64(1), uint64(12345)))),
	},
	{
		query: `
			value type MyType = Int, UnsignedInt[128]
			def output = ^MyType[1, uint[128, 123456789101112131415]]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("MyType", Int64Type, Uint128Type))),
		pdata: xdata("0.arrow", sig(StructType),
			row([]any{int64(1), []uint64{12776324658854821719, 6}})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, BigIntType)),
			row("output", value("MyType", int64(1), NewBigUint128(12776324658854821719, 6)))),
	},
	{
		query: `
			value type MyType = Int, Floating[16]
			def output = ^MyType[1, float[16, 42.5]]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("MyType", Int64Type, Float16Type))),
		pdata: xdata("0.arrow", sig(StructType),
			row([]any{int64(1), float16.New(42.5)})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, Float16Type)),
			row("output", value("MyType", int64(1), float16.New(42.5)))),
	},
	{
		query: `
			value type MyType = Int, Floating[32]
			def output = ^MyType[1, float[32, 42.5]]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("MyType", Int64Type, Float32Type))),
		pdata: xdata("0.arrow", sig(StructType),
			row([]any{int64(1), float32(42.5)})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, Float32Type)),
			row("output", value("MyType", int64(1), float32(42.5)))),
	},
	{
		query: `
			value type MyType = Int, Floating[64]
			def output = ^MyType[1, float[64, 42.5]]`,
		mdata: mdata("0.arrow", sig("output",
			vtype("MyType", Int64Type, Float64Type))),
		pdata: xdata("0.arrow", sig(StructType),
			row([]any{int64(1), float64(42.5)})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, Float64Type)),
			row("output", value("MyType", int64(1), float64(42.5)))),
	},
	{
		query: `
			value type MyType = Int, FixedDecimal[16, 2]
			def output = ^MyType[1, parse_decimal[16, 2, "12.34"]]`,
		mdata: mdata("0.arrow", sig("output", vtype("MyType", Int64Type,
			vtype("rel:base:FixedDecimal", int64(16), int64(2), Int16Type)))),
		pdata: xdata("0.arrow", sig(StructType),
			row([]any{int64(1), int16(1234)})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, DecimalType)),
			row("output", value("MyType", int64(1), decimal.New(1234, -2)))),
	},
	{
		query: `
			value type MyType = Int, FixedDecimal[32, 2]
			def output = ^MyType[1, parse_decimal[32, 2, "12.34"]]`,
		mdata: mdata("0.arrow", sig("output", vtype("MyType", Int64Type,
			vtype("rel:base:FixedDecimal", int64(32), int64(2), Int32Type)))),
		pdata: xdata("0.arrow", sig(StructType),
			row([]any{int64(1), int32(1234)})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, DecimalType)),
			row("output", value("MyType", int64(1), decimal.New(1234, -2)))),
	},
	{
		query: `
			value type MyType = Int, FixedDecimal[64, 2]
			def output = ^MyType[1, parse_decimal[64, 2, "12.34"]]`,
		mdata: mdata("0.arrow", sig("output", vtype("MyType", Int64Type,
			vtype("rel:base:FixedDecimal", int64(64), int64(2), Int64Type)))),
		pdata: xdata("0.arrow", sig(Int64ListType), row([]int64{1, 1234})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, DecimalType)),
			row("output", value("MyType", int64(1), decimal.New(1234, -2)))),
	},
	{
		query: `
			value type MyType = Int, FixedDecimal[128, 2]
			def output = ^MyType[1, parse_decimal[128, 2, "12345678901011121314.34"]]`,
		mdata: mdata("0.arrow", sig("output", vtype("MyType", Int64Type,
			vtype("rel:base:FixedDecimal", int64(128), int64(2), Int128Type)))),
		pdata: xdata("0.arrow", sig(StructType),
			row([]any{int64(1), []uint64{17082781236281724778, 66}})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, DecimalType)),
			row("output", value("MyType", int64(1),
				NewDecimal128(17082781236281724778, 66, -2)))),
	},
	{
		query: `
			value type MyType = Int, Rational[8]
			def output = ^MyType[1, rational[8, 1, 2]]`,
		mdata: mdata("0.arrow", sig("output", vtype("MyType", Int64Type,
			vtype("rel:base:Rational", int64(8), Int8Type, Int8Type)))),
		pdata: xdata("0.arrow", sig(StructType),
			row([]any{int64(1), []int8{1, 2}})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, RationalType)),
			row("output", value("MyType", int64(1), big.NewRat(1, 2)))),
	},
	{
		query: `
			value type MyType = Int, Rational[16]
			def output = ^MyType[1, rational[16, 1, 2]]`,
		mdata: mdata("0.arrow", sig("output", vtype("MyType", Int64Type,
			vtype("rel:base:Rational", int64(16), Int16Type, Int16Type)))),
		pdata: xdata("0.arrow", sig(StructType),
			row([]any{int64(1), []int16{1, 2}})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, RationalType)),
			row("output", value("MyType", int64(1), big.NewRat(1, 2)))),
	},
	{
		query: `
			value type MyType = Int, Rational[32]
			def output = ^MyType[1, rational[32, 1, 2]]`,
		mdata: mdata("0.arrow", sig("output", vtype("MyType", Int64Type,
			vtype("rel:base:Rational", int64(32), Int32Type, Int32Type)))),
		pdata: xdata("0.arrow", sig(StructType),
			row([]any{int64(1), []int32{1, 2}})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, RationalType)),
			row("output", value("MyType", int64(1), big.NewRat(1, 2)))),
	},
	{
		query: `
			value type MyType = Int, Rational[64]
			def output = ^MyType[1, rational[64, 1, 2]]`,
		mdata: mdata("0.arrow", sig("output", vtype("MyType", Int64Type,
			vtype("rel:base:Rational", int64(64), Int64Type, Int64Type)))),
		pdata: xdata("0.arrow", sig(StructType),
			row([]any{int64(1), []int64{1, 2}})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, RationalType)),
			row("output", value("MyType", int64(1), big.NewRat(1, 2)))),
	},
	{
		query: `
			value type MyType = Int, Rational[128]
			def output = ^MyType[1, rational[128, 123456789101112313, 9123456789101112313]]`,
		mdata: mdata("0.arrow", sig("output", vtype("MyType", Int64Type,
			vtype("rel:base:Rational", int64(128), Int128Type, Int128Type)))),
		pdata: xdata("0.arrow", sig(StructType),
			row([]any{int64(1),
				[]uint64{123456789101112313, 0, 9123456789101112313, 0}})),
		rdata: xdata("0.arrow",
			sig("output", vtype("MyType", Int64Type, RationalType)),
			row("output", value("MyType", int64(1), NewRational128(
				NewBigInt128(123456789101112313, 0),
				NewBigInt128(9123456789101112313, 0))))),
	},
}

var extraValueTypeTests = []execTest{
	{
		query: `
			module Foo
				module Bar
					value type MyType = Int, Int
				end
			end
			def output = Foo:Bar:^MyType[12, 34]`,
		mdata: mdata("0.arrow",
			sig("output", vtype("Foo", "Bar", "MyType", Int64Type, Int64Type))),
		pdata: xdata("0.arrow", sig(Int64ListType), row([]int64{12, 34})),
		rdata: xdata("0.arrow",
			sig("output", vtype("Foo", "Bar", "MyType", Int64Type, Int64Type)),
			row("output", value("Foo", "Bar", "MyType", int64(12), int64(34)))),
	},
}

var constValueTypeTests = []execTest{
	/* https://github.com/RelationalAI/raicode/issues/10386
	{
		query: `
			value type MyType = :foo; :bar; :baz
			def v = ^MyType[:foo]
			def output = #(v)`,
	},
	*/
	/* https://github.com/RelationalAI/raicode/issues/10387
	{
		query: `
			value type MyType = Int, String
			def v = ^MyType[1, "abc"]
			def output = #(v)`,
	},
	*/
	// Note, symbols are being dropped from many of these examples
	// https://github.com/RelationalAI/raicode/issues/9578
	{
		query: `
			value type MyType = Int, Boolean
			def v = ^MyType[1, boolean_true]
			def output = #(v)`,
		mdata: mdata("0.arrow", sig("output", ctype("MyType", int64(1), true))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), true)),
			row("output", value("MyType", int64(1), true))),
	},
	{
		query: `
			value type MyType = Int, Boolean
			def v = ^MyType[1, boolean_false]
			def output = #(v)`,
		mdata: mdata("0.arrow", sig("output", ctype("MyType", int64(1), false))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), false)),
			row("output", value("MyType", int64(1), false))),
	},
	{
		query: `
			value type MyType = Int, Char
			def v = ^MyType[1, '👍']
			def output = #(v)`,
		mdata: mdata("0.arrow", sig("output", ctype("MyType", int64(1), '👍'))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), '👍')),
			row("output", value("MyType", int64(1), '👍'))),
	},
	/* https://github.com/RelationalAI/raicode/issues/10396
	{
		query: `
			value type MyType = Int, DateTime
			def v = ^MyType[1, 2021-10-12T01:22:31+10:00]
			def output = #(v)`
	},
	{
		query: `
			value type MyType = Int, Date
			def v = ^MyType[1, 2021-10-12]
			def output = #(v)`
	},
	*/
	{
		query: `
			value type MyType = Int, is_Year
			def v = ^MyType[1, Year[2022]]
			def output = #(v)`,
		mdata: mdata("0.arrow",
			sig("output", ctype("MyType", int64(1), ctype("rel:base:Year", int64(2022))))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), int64(2022))),
			row("output", value("MyType", int64(1), int64(2022)))),
	},
	{
		query: `
			value type MyType = Int, is_Month
			def v = ^MyType[1, Month[2]]
			def output = #(v)`,
		mdata: mdata("0.arrow",
			sig("output", ctype("MyType", int64(1), ctype("rel:base:Month", int64(2))))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), int64(2))),
			row("output", value("MyType", int64(1), int64(2)))),
	},
	{
		query: `
			value type MyType = Int, is_Week
			def v = ^MyType[1, Week[2]]
			def output = #(v)`,
		mdata: mdata("0.arrow",
			sig("output", ctype("MyType", int64(1), ctype("rel:base:Week", int64(2))))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), int64(2))),
			row("output", value("MyType", int64(1), int64(2)))),
	},
	{
		query: `
			value type MyType = Int, is_Day
			def v = ^MyType[1, Day[2]]
			def output = #(v)`,
		mdata: mdata("0.arrow",
			sig("output", ctype("MyType", int64(1), ctype("rel:base:Day", int64(2))))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), int64(2))),
			row("output", value("MyType", int64(1), int64(2)))),
	},
	{
		query: `
			value type MyType = Int, is_Hour
			def v = ^MyType[1, Hour[2]]
			def output = #(v)`,
		mdata: mdata("0.arrow",
			sig("output", ctype("MyType", int64(1), ctype("rel:base:Hour", int64(2))))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), int64(2))),
			row("output", value("MyType", int64(1), int64(2)))),
	},
	{
		query: `
			value type MyType = Int, is_Minute
			def v = ^MyType[1, Minute[2]]
			def output = #(v)`,
		mdata: mdata("0.arrow",
			sig("output", ctype("MyType", int64(1), ctype("rel:base:Minute", int64(2))))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), int64(2))),
			row("output", value("MyType", int64(1), int64(2)))),
	},
	{
		query: `
			value type MyType = Int, is_Second
			def v = ^MyType[1, Second[2]]
			def output = #(v)`,
		mdata: mdata("0.arrow",
			sig("output", ctype("MyType", int64(1), ctype("rel:base:Second", int64(2))))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), int64(2))),
			row("output", value("MyType", int64(1), int64(2)))),
	},
	{
		query: `
			value type MyType = Int, is_Millisecond
			def v = ^MyType[1, Millisecond[2]]
			def output = #(v)`,
		mdata: mdata("0.arrow",
			sig("output", ctype("MyType", int64(1), ctype("rel:base:Millisecond", int64(2))))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), int64(2))),
			row("output", value("MyType", int64(1), int64(2)))),
	},
	{
		query: `
			value type MyType = Int, is_Microsecond
			def v = ^MyType[1, Microsecond[2]]
			def output = #(v)`,
		mdata: mdata("0.arrow", sig("output", ctype("MyType", int64(1),
			ctype("rel:base:Microsecond", int64(2))))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), int64(2))),
			row("output", value("MyType", int64(1), int64(2)))),
	},
	{
		query: `
			value type MyType = Int, is_Nanosecond
			def v = ^MyType[1, Nanosecond[2]]
			def output = #(v)`,
		mdata: mdata("0.arrow",
			sig("output", ctype("MyType", int64(1), ctype("rel:base:Nanosecond", int64(2))))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), int64(2))),
			row("output", value("MyType", int64(1), int64(2)))),
	},
	{
		query: `
			value type MyType = Int, Hash
			def h(x) = hash128["abc", _, x]
			def v = ^MyType[1, h]
			def output = #(v)`,
		mdata: mdata("0.arrow",
			sig("output", ctype("MyType", int64(1), ctype("rel:base:Hash",
				NewBigUint128(3877405323480549948, 3198683864092244389))))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1),
				NewBigUint128(3877405323480549948, 3198683864092244389))),
			row("output", value("MyType", int64(1),
				NewBigUint128(3877405323480549948, 3198683864092244389)))),
	},
	/* possible dup of: https://github.com/RelationalAI/raicode/issues/10387
	{
		query: `
			value type MyType = Int, Missing
			def v = ^MyType[1, missing]
			def output = #(v)`,
	},
	*/
	{
		query: `
			def config:data="""
			a,b,c
			1,2,3"""
			def csv = load_csv[config]
			def f(p) = csv(_, p, _)
			value type MyType = Int, FilePos
			def v = ^MyType[1, f]
			def output = #(v)`,
		mdata: mdata("0.arrow",
			sig("output", ctype("MyType", int64(1), ctype("rel:base:FilePos", int64(2))))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), int64(2))),
			row("output", value("MyType", int64(1), int64(2)))),
	},
	{
		query: `
			value type MyType = Int, SignedInt[8]
			def v = ^MyType[1, int[8, -12]]
			def output = #(v)`,
		mdata: mdata("0.arrow", sig("output", ctype("MyType", int64(1), int8(-12)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), int8(-12))),
			row("output", value("MyType", int64(1), int8(-12)))),
	},
	{
		query: `
			value type MyType = Int, SignedInt[16]
			def v = ^MyType[1, int[16, -123]]
			def output = #(v)`,
		mdata: mdata("0.arrow", sig("output", ctype("MyType", int64(1), int16(-123)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), int16(-123))),
			row("output", value("MyType", int64(1), int16(-123)))),
	},
	{
		query: `
			value type MyType = Int, SignedInt[32]
			def v = ^MyType[1, int[32, -1234]]
			def output = #(v)`,
		mdata: mdata("0.arrow", sig("output", ctype("MyType", int64(1), int32(-1234)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), int32(-1234))),
			row("output", value("MyType", int64(1), int32(-1234)))),
	},
	{
		query: `
			value type MyType = Int, SignedInt[64]
			def v = ^MyType[1, int[64, -12345]]
			def output = #(v)`,
		mdata: mdata("0.arrow", sig("output", ctype("MyType", int64(1), int64(-12345)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), int64(-12345))),
			row("output", value("MyType", int64(1), int64(-12345)))),
	},
	{
		query: `
			value type MyType = Int, SignedInt[128]
			def v = ^MyType[1, int[128, 123456789101112131415]]
			def output = #(v)`,
		mdata: mdata("0.arrow",
			sig("output", ctype("MyType", int64(1), NewBigInt128(12776324658854821719, 6)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), NewBigInt128(12776324658854821719, 6))),
			row("output", value("MyType", int64(1), NewBigInt128(12776324658854821719, 6)))),
	},
	{
		query: `
			value type MyType = Int, UnsignedInt[8]
			def v = ^MyType[1, uint[8, 12]]
			def output = #(v)`,
		mdata: mdata("0.arrow", sig("output", ctype("MyType", int64(1), uint8(12)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), uint8(12))),
			row("output", value("MyType", int64(1), uint8(12)))),
	},
	{
		query: `
			value type MyType = Int, UnsignedInt[16]
			def v = ^MyType[1, uint[16, 123]]
			def output = #(v)`,
		mdata: mdata("0.arrow", sig("output", ctype("MyType", int64(1), uint16(123)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), uint16(123))),
			row("output", value("MyType", int64(1), uint16(123)))),
	},
	{
		query: `
			value type MyType = Int, UnsignedInt[32]
			def v = ^MyType[1, uint[32, 1234]]
			def output = #(v)`,
		mdata: mdata("0.arrow", sig("output", ctype("MyType", int64(1), uint32(1234)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), uint32(1234))),
			row("output", value("MyType", int64(1), uint32(1234)))),
	},
	{
		query: `
			value type MyType = Int, UnsignedInt[64]
			def v = ^MyType[1, uint[64, 12345]]
			def output = #(v)`,
		mdata: mdata("0.arrow", sig("output", ctype("MyType", int64(1), uint64(12345)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), uint64(12345))),
			row("output", value("MyType", int64(1), uint64(12345)))),
	},
	{
		query: `
			value type MyType = Int, UnsignedInt[128]
			def v = ^MyType[1, uint[128, 123456789101112131415]]
			def output = #(v)`,
		mdata: mdata("0.arrow",
			sig("output", ctype("MyType", int64(1), NewBigUint128(12776324658854821719, 6)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), NewBigUint128(12776324658854821719, 6))),
			row("output", value("MyType", int64(1), NewBigUint128(12776324658854821719, 6)))),
	},
	{
		query: `
			value type MyType = Int, Floating[16]
			def v = ^MyType[1, float[16, 42.5]]
			def output = #(v)`,
		mdata: mdata("0.arrow", sig("output", ctype("MyType", int64(1), float16.New(42.5)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), float16.New(42.5))),
			row("output", value("MyType", int64(1), float16.New(42.5)))),
	},
	{
		query: `
			value type MyType = Int, Floating[32]
			def v = ^MyType[1, float[32, 42.5]]
			def output = #(v)`,
		mdata: mdata("0.arrow", sig("output", ctype("MyType", int64(1), float32(42.5)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), float32(42.5))),
			row("output", value("MyType", int64(1), float32(42.5)))),
	},
	{
		query: `
			value type MyType = Int, Floating[64]
			def v = ^MyType[1, float[64, 42.5]]
			def output = #(v)`,
		mdata: mdata("0.arrow", sig("output", ctype("MyType", int64(1), float64(42.5)))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), float64(42.5))),
			row("output", value("MyType", int64(1), float64(42.5)))),
	},
	{
		query: `
			value type MyType = Int, FixedDecimal[16, 2]
			def v = ^MyType[1, parse_decimal[16, 2, "12.34"]]
			def output = #(v)`,
		mdata: mdata("0.arrow",
			sig("output", ctype("MyType", int64(1),
				ctype("rel:base:FixedDecimal", int64(16), int64(2), int16(1234))))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), decimal.New(1234, -2))),
			row("output", value("MyType", int64(1), decimal.New(1234, -2)))),
	},
	{
		query: `
			value type MyType = Int, FixedDecimal[32, 2]
			def v = ^MyType[1, parse_decimal[32, 2, "12.34"]]
			def output = #(v)`,
		mdata: mdata("0.arrow",
			sig("output", ctype("MyType", int64(1),
				ctype("rel:base:FixedDecimal", int64(32), int64(2), int32(1234))))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), decimal.New(1234, -2))),
			row("output", value("MyType", int64(1), decimal.New(1234, -2)))),
	},
	{
		query: `
			value type MyType = Int, FixedDecimal[64, 2]
			def v = ^MyType[1, parse_decimal[64, 2, "12.34"]]
			def output = #(v)`,
		mdata: mdata("0.arrow",
			sig("output", ctype("MyType", int64(1),
				ctype("rel:base:FixedDecimal", int64(64), int64(2), int64(1234))))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), decimal.New(1234, -2))),
			row("output", value("MyType", int64(1), decimal.New(1234, -2)))),
	},
	{
		query: `
			value type MyType = Int, FixedDecimal[128, 2]
			def v = ^MyType[1, parse_decimal[128, 2, "12345678901011121314.34"]]
			def output = #(v)`,
		mdata: mdata("0.arrow",
			sig("output", ctype("MyType", int64(1),
				ctype("rel:base:FixedDecimal", int64(128), int64(2),
					NewBigInt128(17082781236281724778, 66))))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), NewDecimal128(17082781236281724778, 66, -2))),
			row("output", value("MyType", int64(1), NewDecimal128(17082781236281724778, 66, -2)))),
	},
	{
		query: `
			value type MyType = Int, Rational[8]
			def v = ^MyType[1, rational[8, 1, 2]]
			def output = #(v)`,
		mdata: mdata("0.arrow",
			sig("output", ctype("MyType", int64(1),
				ctype("rel:base:Rational", int64(8), int8(1), int8(2))))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), big.NewRat(int64(1), int64(2)))),
			row("output", value("MyType", int64(1), big.NewRat(int64(1), int64(2))))),
	},
	{
		query: `
			value type MyType = Int, Rational[16]
			def v = ^MyType[1, rational[16, 1, 2]]
			def output = #(v)`,
		mdata: mdata("0.arrow",
			sig("output", ctype("MyType", int64(1),
				ctype("rel:base:Rational", int64(16), int16(1), int16(2))))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), big.NewRat(int64(1), int64(2)))),
			row("output", value("MyType", int64(1), big.NewRat(int64(1), int64(2))))),
	},
	{
		query: `
			value type MyType = Int, Rational[32]
			def v = ^MyType[1, rational[32, 1, 2]]
			def output = #(v)`,
		mdata: mdata("0.arrow",
			sig("output", ctype("MyType", int64(1),
				ctype("rel:base:Rational", int64(32), int32(1), int32(2))))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), big.NewRat(int64(1), int64(2)))),
			row("output", value("MyType", int64(1), big.NewRat(int64(1), int64(2))))),
	},
	{
		query: `
			value type MyType = Int, Rational[64]
			def v = ^MyType[1, rational[64, 1, 2]]
			def output = #(v)`,
		mdata: mdata("0.arrow",
			sig("output", ctype("MyType", int64(1),
				ctype("rel:base:Rational", int64(64), int64(1), int64(2))))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), big.NewRat(int64(1), int64(2)))),
			row("output", value("MyType", int64(1), big.NewRat(int64(1), int64(2))))),
	},
	{
		query: `
			value type MyType = Int, Rational[128]
			def v = ^MyType[1, rational[128, 123456789101112313, 9123456789101112313]]
			def output = #(v)`,
		mdata: mdata("0.arrow",
			sig("output", ctype("MyType", int64(1),
				ctype("rel:base:Rational", int64(128),
					NewBigInt128(123456789101112313, 0),
					NewBigInt128(9123456789101112313, 0))))),
		pdata: xdata("0.arrow", sig(), row()),
		rdata: xdata("0.arrow",
			sig("output", ctype("MyType", int64(1), NewRational128(
				NewBigInt128(123456789101112313, 0),
				NewBigInt128(9123456789101112313, 0)))),
			row("output", value("MyType", int64(1), NewRational128(
				NewBigInt128(123456789101112313, 0),
				NewBigInt128(9123456789101112313, 0))))),
	},
}

// todo: entity permutations (all primitive & constant types)
// todo: constant entity types

// Remove any leading whitespace from all lines in given string.
func dindent(s string) string {
	result := []string{}
	for _, s := range strings.Split(s, "\n") {
		s = strings.TrimSpace(s)
		if len(s) > 0 {
			result = append(result, s)
		}
	}
	return strings.Join(result, "\n")
}

// Check that the expected values match the values in the given columns.
func checkColumns(t *testing.T, x [][]any, cols []Column) {
	if x == nil {
		return // no expected value to check
	}
	assert.Equal(t, len(x), len(cols))
	ncols := len(x)
	if ncols == 0 {
		return
	}
	nrows := len(x[0])
	for cnum := 0; cnum < ncols; cnum++ {
		assert.Equal(t, nrows, cols[cnum].NumRows())
	}
	for rnum := 0; rnum < nrows; rnum++ {
		for cnum := 0; cnum < ncols; cnum++ {
			xv := x[cnum][rnum]
			cv := cols[cnum].Value(rnum)
			assert.Equal(t, xv, cv)
		}
	}
}

// Check that the response matches what is expected by the given test.
func checkResponse(t *testing.T, test execTest, rsp *TransactionResponse) {
	if test.mdata != nil {
		assert.Equal(t, test.mdata, rsp.Metadata.Signatures())
	}
	if test.pdata != nil {
		for id, p := range rsp.Partitions {
			xp := test.pdata[id]
			assert.Equal(t, xp.sig, p.Signature())
			checkColumns(t, xp.cols, p.Columns())
		}
	}
	if test.rdata != nil {
		for id := range rsp.Partitions {
			xr := test.rdata[id]
			r := rsp.Relation(id)
			assert.Equal(t, xr.sig, r.Signature())
			checkColumns(t, xr.cols, r.Columns())
		}
	}
}

func runTests(t *testing.T, tests []execTest) {
	for _, tst := range tests {
		q := dindent(tst.query)
		if test.showQuery {
			fmt.Println(q) // useful for debugging tests
		}
		rsp, err := test.client.Execute(test.databaseName, test.engineName, q, nil, true, o11yTag)
		assert.Nil(t, err)
		checkResponse(t, tst, rsp)
	}
}

func TestPrimitiveTypes(t *testing.T) {
	runTests(t, primitiveTypeTests)
}

func TestConstPrimitiveTypes(t *testing.T) {
	runTests(t, constPrimitiveTypeTests)
}

func TestConstValueTypes(t *testing.T) {
	runTests(t, constValueTypeTests)
}

func TestValueTypes(t *testing.T) {
	runTests(t, valueTypeTests)
	runTests(t, extraValueTypeTests)
}

func TestInterfaceTypes(t *testing.T) {
	var c Column

	c = boolColumn{}
	_ = c.(SimpleColumn[bool])

	c = float16Column{}
	_ = c.(SimpleColumn[float16.Num])

	c = primitiveColumn[float32]{}
	_ = c.(SimpleColumn[float32])

	c = primitiveColumn[float64]{}
	_ = c.(SimpleColumn[float64])

	c = stringColumn{}
	_ = c.(SimpleColumn[string])

	c = listColumn[float64]{}
	_ = c.(TabularColumn[float64])
	_ = c.(Tabular)

	c = listColumn[int8]{}
	_ = c.(TabularColumn[int8])
	_ = c.(Tabular)

	c = listColumn[int16]{}
	_ = c.(TabularColumn[int16])
	_ = c.(Tabular)

	c = listColumn[int32]{}
	_ = c.(TabularColumn[int32])
	_ = c.(Tabular)

	c = listColumn[int64]{}
	_ = c.(TabularColumn[int64])
	_ = c.(Tabular)

	c = listColumn[uint64]{}
	_ = c.(TabularColumn[uint64])
	_ = c.(Tabular)

	c = structColumn{}
	_ = c.(TabularColumn[any])
	_ = c.(Tabular)

	c = unknownColumn{}
	_ = c.(SimpleColumn[string])

	c = charColumn{}
	_ = c.(SimpleColumn[rune])

	c = dateColumn{}
	_ = c.(SimpleColumn[time.Time])

	c = dateTimeColumn{}
	_ = c.(SimpleColumn[time.Time])

	c = decimal8Column{}
	_ = c.(SimpleColumn[decimal.Decimal])

	c = decimal16Column{}
	_ = c.(SimpleColumn[decimal.Decimal])

	c = decimal32Column{}
	_ = c.(SimpleColumn[decimal.Decimal])

	c = decimal64Column{}
	_ = c.(SimpleColumn[decimal.Decimal])

	c = decimal128Column{}
	_ = c.(SimpleColumn[decimal.Decimal])

	c = int128Column{}
	_ = c.(SimpleColumn[*big.Int])

	c = uint128Column{}
	_ = c.(SimpleColumn[*big.Int])

	c = literalColumn[bool]{}
	_ = c.(SimpleColumn[bool])

	c = rational8Column{}
	_ = c.(SimpleColumn[*big.Rat])

	c = rational16Column{}
	_ = c.(SimpleColumn[*big.Rat])

	c = rational32Column{}
	_ = c.(SimpleColumn[*big.Rat])

	c = rational64Column{}
	_ = c.(SimpleColumn[*big.Rat])

	c = rational128Column{}
	_ = c.(SimpleColumn[*big.Rat])

	c = symbolColumn{}
	_ = c.(SimpleColumn[string])

	c = missingColumn{}
	_ = c.(SimpleColumn[string])

	c = constColumn{}
	_ = c.(TabularColumn[any])
	_ = c.(Tabular)

	c = valueColumn{}
	_ = c.(TabularColumn[any])
	_ = c.(Tabular)

	c = nilColumn{}
	_ = c.(DataColumn[any])

	c = unionColumn{}
	_ = c.(DataColumn[any])

	var p any = (&Partition{})
	_ = p.(TabularColumn[any])
	_ = p.(Tabular)

	var r Relation
	r = &baseRelation{}
	_ = r.(TabularColumn[any])
	_ = r.(Tabular)

	r = &derivedRelation{}
	_ = r.(TabularColumn[any])
	_ = r.(Tabular)
}

func TestPrefixMatch(t *testing.T) {
	query := `def output = 1, :foo, "a"; 42, :bar, "c"`

	rsp, err := test.client.Execute(test.databaseName, test.engineName, dindent(query), nil, true, o11yTag)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(rsp.Relations()))

	// select relations by ID
	rel := rsp.Relation("0.arrow")
	assert.Equal(t, 4, rel.NumCols())
	assert.Equal(t, 1, rel.NumRows())
	rel = rsp.Relation("1.arrow")
	assert.Equal(t, 4, rel.NumCols())
	assert.Equal(t, 1, rel.NumRows())

	// select relations by signature prefix
	rs := rsp.Relations("output")
	assert.Equal(t, 2, len(rs))
	rel = rs.Union()
	assert.Equal(t, 4, rel.NumCols())
	assert.Equal(t, 2, rel.NumRows())

	rs = rsp.Relations("output", Int64Type)
	assert.Equal(t, 2, len(rs))
	rel = rs.Union()
	assert.Equal(t, 4, rel.NumCols())
	assert.Equal(t, 2, rel.NumRows())

	rs = rsp.Relations("output", Int64Type, "foo")
	assert.Equal(t, 1, len(rs))
	rel = rs.Union()
	assert.Equal(t, 4, rel.NumCols())
	assert.Equal(t, 1, rel.NumRows())

	rs = rsp.Relations("output", Int64Type, "bar")
	assert.Equal(t, 1, len(rs))
	rel = rs.Union()
	assert.Equal(t, 4, rel.NumCols())
	assert.Equal(t, 1, rel.NumRows())

	rs = rsp.Relations("output", Int64Type, "_", StringType)
	assert.Equal(t, 2, len(rs))
	rel = rs.Union()
	assert.Equal(t, 4, rel.NumCols())
	assert.Equal(t, 2, rel.NumRows())

	rs = rsp.Relations("nonsense")
	assert.Equal(t, 0, len(rs))
}

// Pick one row where the given column matches the given value.
func pick(r Relation, ncol int, v any) []any {
	for nrow := 0; nrow < r.NumRows(); nrow++ {
		row := r.Row(nrow)
		if row[ncol] == v {
			return row
		}
	}
	return nil
}

func TestRelationSlice(t *testing.T) {
	query := `
		def output =
			1, :foo, "a";
			2, :bar, "c";
			3, :baz, 42;
			4, :cat, #(42);
			5, :bip, 3.14, "pi!";
			6, :zip, missing, "pip"`

	rsp, err := test.client.Execute(test.databaseName, test.engineName, dindent(query), nil, true, o11yTag)
	assert.Nil(t, err)
	assert.Equal(t, 6, len(rsp.Relations()))

	rel := rsp.Relation("0.arrow")
	assert.Equal(t, 4, rel.NumCols())
	assert.Equal(t, 1, rel.NumRows())
	assert.Equal(t, sig("output", Int64Type, "cat", int64(42)), rel.Signature())
	assert.Equal(t, []any{"output", int64(4), "cat", int64(42)}, rel.Row(0))

	rel = rel.Slice(1)
	assert.Equal(t, 3, rel.NumCols())
	assert.Equal(t, 1, rel.NumRows())
	assert.Equal(t, sig(Int64Type, "cat", int64(42)), rel.Signature())
	assert.Equal(t, []any{int64(4), "cat", int64(42)}, rel.Row(0))

	rel = rel.Slice(0, rel.NumCols()-1)
	assert.Equal(t, 2, rel.NumCols())
	assert.Equal(t, 1, rel.NumRows())
	assert.Equal(t, sig(Int64Type, "cat"), rel.Signature())
	assert.Equal(t, []any{int64(4), "cat"}, rel.Row(0))

	rel = rsp.Relations().Union().Slice(1)
	assert.Equal(t, 4, rel.NumCols())
	assert.Equal(t, 6, rel.NumRows())
	assert.Equal(t, sig(Int64Type, MixedType, MixedType, MixedType), rel.Signature())
	r := pick(rel, 0, int64(1))
	assert.Equal(t, 4, len(r))
	assert.Equal(t, []any{int64(1), "foo", "a", nil}, r)
	r = pick(rel, 0, int64(2))
	assert.Equal(t, 4, len(r))
	assert.Equal(t, []any{int64(2), "bar", "c", nil}, r)
	r = pick(rel, 0, int64(3))
	assert.Equal(t, 4, len(r))
	assert.Equal(t, []any{int64(3), "baz", int64(42), nil}, r)
	r = pick(rel, 0, int64(4))
	assert.Equal(t, 4, len(r))
	assert.Equal(t, []any{int64(4), "cat", int64(42), nil}, r)
	r = pick(rel, 0, int64(5))
	assert.Equal(t, 4, len(r))
	assert.Equal(t, []any{int64(5), "bip", 3.14, "pi!"}, r)
	r = pick(rel, 0, int64(6))
	assert.Equal(t, 4, len(r))
	assert.Equal(t, []any{int64(6), "zip", "missing", "pip"}, r)

	rel = rel.Slice(0, rel.NumCols()-1)
	assert.Equal(t, 3, rel.NumCols())
	assert.Equal(t, 6, rel.NumRows())
	assert.Equal(t, sig(Int64Type, MixedType, MixedType), rel.Signature())
	r = pick(rel, 0, int64(1))
	assert.Equal(t, 3, len(r))
	assert.Equal(t, []any{int64(1), "foo", "a"}, r)
	r = pick(rel, 0, int64(2))
	assert.Equal(t, 3, len(r))
	assert.Equal(t, []any{int64(2), "bar", "c"}, r)
	r = pick(rel, 0, int64(3))
	assert.Equal(t, 3, len(r))
	assert.Equal(t, []any{int64(3), "baz", int64(42)}, r)
	r = pick(rel, 0, int64(4))
	assert.Equal(t, 3, len(r))
	assert.Equal(t, []any{int64(4), "cat", int64(42)}, r)
	r = pick(rel, 0, int64(5))
	assert.Equal(t, 3, len(r))
	assert.Equal(t, []any{int64(5), "bip", 3.14}, r)
	r = pick(rel, 0, int64(6))
	assert.Equal(t, 3, len(r))
	assert.Equal(t, []any{int64(6), "zip", "missing"}, r)
}

func TestRelationUnion(t *testing.T) {
	query := `
		def output =
			1, :foo, "a";
			2, :bar, "c";
			3, :baz, 42;
			4, :cat, #(42);
			5, :bip, 3.14, "pi!";
			6, :zip, missing, "pip"`

	rsp, err := test.client.Execute(test.databaseName, test.engineName, dindent(query), nil, true, o11yTag)
	assert.Nil(t, err)
	assert.Equal(t, 6, len(rsp.Relations()))

	rel := rsp.Relation("0.arrow")
	assert.Equal(t, 4, rel.NumCols())
	assert.Equal(t, 1, rel.NumRows())
	assert.Equal(t, sig("output", Int64Type, "cat", int64(42)), rel.Signature())
	assert.Equal(t, []any{"output", int64(4), "cat", int64(42)}, rel.Row(0))

	rel = rsp.Relation("1.arrow")
	assert.Equal(t, 4, rel.NumCols())
	assert.Equal(t, 1, rel.NumRows())
	assert.Equal(t, sig("output", Int64Type, "bar", StringType), rel.Signature())
	assert.Equal(t, []any{"output", int64(2), "bar", "c"}, rel.Row(0))

	rel = rsp.Relation("2.arrow")
	assert.Equal(t, 4, rel.NumCols())
	assert.Equal(t, 1, rel.NumRows())
	assert.Equal(t, sig("output", Int64Type, "baz", Int64Type), rel.Signature())
	assert.Equal(t, []any{"output", int64(3), "baz", int64(42)}, rel.Row(0))

	rel = rsp.Relation("3.arrow")
	assert.Equal(t, 5, rel.NumCols())
	assert.Equal(t, 1, rel.NumRows())
	assert.Equal(t, sig("output", Int64Type, "bip", Float64Type, StringType), rel.Signature())
	assert.Equal(t, []any{"output", int64(5), "bip", 3.14, "pi!"}, rel.Row(0))

	rel = rsp.Relation("4.arrow")
	assert.Equal(t, 4, rel.NumCols())
	assert.Equal(t, 1, rel.NumRows())
	assert.Equal(t, sig("output", Int64Type, "foo", StringType), rel.Signature())
	assert.Equal(t, []any{"output", int64(1), "foo", "a"}, rel.Row(0))

	rel = rsp.Relation("5.arrow")
	assert.Equal(t, 5, rel.NumCols())
	assert.Equal(t, 1, rel.NumRows())
	assert.Equal(t, sig("output", Int64Type, "zip", MissingType, StringType), rel.Signature())
	assert.Equal(t, []any{"output", int64(6), "zip", "missing", "pip"}, rel.Row(0))

	rel = rsp.Relations().Union()
	assert.Equal(t, 5, rel.NumCols())
	assert.Equal(t, sig("output", Int64Type, MixedType, MixedType, MixedType), rel.Signature())
	assert.Equal(t, 6, rel.NumRows())
	r := pick(rel, 1, int64(1))
	assert.Equal(t, 5, len(r))
	assert.Equal(t, []any{"output", int64(1), "foo", "a", nil}, r)
	r = pick(rel, 1, int64(2))
	assert.Equal(t, 5, len(r))
	assert.Equal(t, []any{"output", int64(2), "bar", "c", nil}, r)
	r = pick(rel, 1, int64(3))
	assert.Equal(t, 5, len(r))
	assert.Equal(t, []any{"output", int64(3), "baz", int64(42), nil}, r)
	r = pick(rel, 1, int64(4))
	assert.Equal(t, 5, len(r))
	assert.Equal(t, []any{"output", int64(4), "cat", int64(42), nil}, r)
	r = pick(rel, 1, int64(5))
	assert.Equal(t, 5, len(r))
	assert.Equal(t, []any{"output", int64(5), "bip", 3.14, "pi!"}, r)
	r = pick(rel, 1, int64(6))
	assert.Equal(t, 5, len(r))
	assert.Equal(t, []any{"output", int64(6), "zip", "missing", "pip"}, r)
}
