// Copyright 2022 RelationalAI, Inc.

package rai

// Support for accessing transaction results.

// Transaction results consist of the transaction resource, relations and
// metadata describing those relations. The relations may be output relations
// and/or relations describing problems with the transaction. The metadata is
// encoded using protobuf, and all relations are encoded using Apache Arrow.
//
// RelationalAI represents relations in a low-level physical format known as a
// Partition, where constant values are lifted into the metadata, and the
// relation data is partitioned by the resulting unique metadata signatures.
// For example:
//
//     def output = 1, :foo; 2, :bar; 3, :baz
//
//  results in 3 partitions, each with a unique metadata signature, and in this
//  example, a single column with a single row of data:
//
//     sig: (Int64, :foo), data: [[1]]
//     sig: (Int64, :bar), data: [[2]]
//     sig: (Int64, :baz), data: [[3]]
//
// This representation eliminats the duplication of constant values.
//
// This file provides accessors for the raw partition data, accessors for
// projections of that data back to its relational form (with constants
// restored to value space), and operations for combining and projecting
// relations.

import (
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
	"github.com/apache/arrow/go/v7/arrow/float16"
	"github.com/shopspring/decimal"
)

type floatTypes interface {
	float16.Num | float32 | float64
}

type intTypes interface {
	int8 | int16 | int32 | int64 | uint8 | uint16 | uint32 | uint64
}

type primTypes interface {
	bool | floatTypes | intTypes | string
}

func asString(v any) string {
	switch vv := v.(type) {
	case rune:
		return fmt.Sprintf("'%c'", vv)
	case string:
		return fmt.Sprintf("\"%s\"", vv)
	case time.Time:
		return vv.Format(time.RFC3339)
	default:
		return fmt.Sprintf("%v", vv)
	}
}

// Answers if the given signature has a prefix that matches the given terms,
// where '_' is a single term wildcard.
func matchPrefix(sig []any, terms ...string) bool {
	n := len(terms)
	if len(sig) < n {
		return false
	}
	for i, term := range terms {
		s, ok := sig[i].(string)
		if !ok {
			return false
		}
		if term == "_" {
			continue
		}
		if s != term {
			return false
		}
	}
	return true
}

// Column is the standard interface to a single column of data.
type Column interface {
	NumRows() int
	String(int) string
	Type() any // reflect.Type | UnionType
	Value(int) any
}

// DataColumn is the standard interface to a single, typed column of data.
type DataColumn[T any] interface {
	Column
	GetItem(int, *T)
	Item(int) T
}

// Tabular is the standard interface to a sequence of columns of data.
type Tabular interface {
	Column
	Column(int) Column
	Columns() []Column
	NumCols() int
	GetRow(int, []any)
	Row(int) []any
	Signature() Signature
	Strings(int) []string
}

type PrimitiveColumn[T primTypes] struct {
	data []T
}

func (c PrimitiveColumn[T]) GetItem(rnum int, out *T) {
	*out = c.data[rnum]
}

func (c PrimitiveColumn[T]) Item(rnum int) T {
	return c.data[rnum]
}

func (c PrimitiveColumn[T]) NumRows() int {
	return len(c.data)
}

func (c PrimitiveColumn[T]) String(rnum int) string {
	return fmt.Sprintf("%v", c.data[rnum])
}

func (c PrimitiveColumn[T]) Type() any {
	return typeOf[T]()
}

func (c PrimitiveColumn[T]) Value(rnum int) any {
	return c.data[rnum]
}

// Sadly, the `array.Boolean` type does not have a `Values` accessor.
type BoolColumn struct {
	data *array.Boolean
}

func newBoolColumn(data *array.Boolean) BoolColumn {
	return BoolColumn{data}
}

func (c BoolColumn) GetItem(rnum int, out *bool) {
	*out = c.data.Value(rnum)
}

func (c BoolColumn) Item(rnum int) bool {
	return c.data.Value(rnum)
}

func (c BoolColumn) NumRows() int {
	return c.data.Len()
}

func (c BoolColumn) String(rnum int) string {
	return strconv.FormatBool(c.data.Value(rnum))
}

func (c BoolColumn) Type() any {
	return BoolType
}

func (c BoolColumn) Value(rnum int) any {
	return c.data.Value(rnum)
}

type Float16Column struct {
	PrimitiveColumn[float16.Num]
}

func newFloat16Column(data []float16.Num) Float16Column {
	return Float16Column{PrimitiveColumn[float16.Num]{data}}
}

func (c Float16Column) Item(rnum int) float16.Num {
	return c.data[rnum]
}

func (c Float16Column) Type() any {
	return Float16Type
}

func newFloat32Column(data []float32) PrimitiveColumn[float32] {
	return PrimitiveColumn[float32]{data}
}

func newFloat64Column(data []float64) PrimitiveColumn[float64] {
	return PrimitiveColumn[float64]{data}
}

// Sadly, the `array.String“ type does not have a `Values` accessor.
type StringColumn struct {
	data *array.String
}

func newStringColumn(data *array.String) StringColumn {
	return StringColumn{data}
}

func (c StringColumn) GetItem(rnum int, out *string) {
	*out = c.data.Value(rnum)
}

func (c StringColumn) Item(rnum int) string {
	return c.data.Value(rnum)
}

func (c StringColumn) NumRows() int {
	return c.data.Len()
}

func (c StringColumn) String(rnum int) string {
	return c.data.Value(rnum)
}

func (c StringColumn) Type() any {
	return StringType
}

func (c StringColumn) Value(rnum int) any {
	return c.data.Value(rnum)
}

type ListColumn[T any] struct {
	data  []T // raw arrow data
	ncols int
	cols  []Column
}

func newListColumn(c *array.FixedSizeList) Column {
	col := c.ListValues()
	nrows := c.Len()
	nvals := col.Len()
	ncols := nvals / nrows
	switch cc := col.(type) {
	case *array.Float64:
		return ListColumn[float64]{cc.Float64Values(), ncols, nil}
	case *array.Int8:
		return ListColumn[int8]{cc.Int8Values(), ncols, nil}
	case *array.Int16:
		return ListColumn[int16]{cc.Int16Values(), ncols, nil}
	case *array.Int32:
		return ListColumn[int32]{cc.Int32Values(), ncols, nil}
	case *array.Int64:
		return ListColumn[int64]{cc.Int64Values(), ncols, nil}
	case *array.Uint64:
		return ListColumn[uint64]{cc.Uint64Values(), ncols, nil}
	case *array.FixedSizeList: // Rational128
		ccv := cc.ListValues().(*array.Uint64)
		return ListColumn[uint64]{ccv.Uint64Values(), 4, nil}
	}
	return newUnknownColumn(nrows)
}

// ListColumn:DataColumn

func (c ListColumn[T]) GetItem(rnum int, out []T) {
	roffs := rnum * c.ncols
	for cnum := 0; cnum < c.ncols; cnum++ {
		out[cnum] = c.data[roffs+cnum]
	}
}

func (c ListColumn[T]) Item(rnum int) []T {
	result := make([]T, c.ncols)
	c.GetItem(rnum, result)
	return result
}

func (c ListColumn[T]) NumRows() int {
	return len(c.data) / c.ncols
}

func (c ListColumn[T]) String(rnum int) string {
	return "(" + strings.Join(c.Strings(rnum), ", ") + ")"
}

func (c ListColumn[T]) Type() any {
	return reflect.TypeOf(*new([]T))
}

func (c ListColumn[T]) Value(rnum int) any {
	return c.Item(rnum)
}

// ListColumn:Tabular

func (c ListColumn[T]) Column(cnum int) Column {
	return ListItemColumn[T]{c.data, cnum, c.ncols}
}

func (c ListColumn[T]) Columns() []Column {
	if c.cols == nil {
		c.cols = make([]Column, c.ncols)
		for i := 0; i < c.ncols; i++ {
			c.cols[i] = ListItemColumn[T]{c.data, c.ncols, i}
		}
	}
	return c.cols
}

func (c ListColumn[T]) NumCols() int {
	return c.ncols
}

func (c ListColumn[T]) GetRow(rnum int, out []any) {
	roffs := rnum * c.ncols
	for cnum := 0; cnum < c.ncols; cnum++ {
		out[cnum] = c.data[roffs+cnum]
	}
}

func (c ListColumn[T]) Row(rnum int) []any {
	result := make([]any, c.ncols)
	c.GetRow(rnum, result)
	return result
}

func (c ListColumn[T]) Signature() Signature {
	t := typeOf[T]()
	result := make([]any, c.ncols)
	for i := 0; i < c.ncols; i++ {
		result[i] = t
	}
	return result
}

func (c ListColumn[T]) Strings(rnum int) []string {
	roffs := rnum * c.ncols
	result := make([]string, c.ncols)
	for cnum := 0; cnum < c.ncols; cnum++ {
		result[cnum] = asString(c.data[roffs+cnum])
	}
	return result
}

// Represents one item of a `ListColumn`
type ListItemColumn[T any] struct {
	data  []T
	cnum  int
	ncols int
}

func (c ListItemColumn[T]) GetItem(rnum int, out *T) {
	*out = c.data[(rnum*c.ncols)+c.cnum]
}

func (c ListItemColumn[T]) Item(rnum int) T {
	return c.data[(rnum*c.ncols)+c.cnum]
}

func (c ListItemColumn[T]) NumRows() int {
	return len(c.data) / c.ncols
}

func (c ListItemColumn[T]) String(rnum int) string {
	return asString(c.Item(rnum))
}

func (c ListItemColumn[T]) Type() any {
	return typeOf[T]()
}

func (c ListItemColumn[T]) Value(rnum int) any {
	return c.Item(rnum)
}

type StructColumn struct {
	cols []Column
}

// Note, its possible for a `StructColumn` to be empty.
func newStructColumn(c *array.Struct) StructColumn {
	ncols := c.NumField()
	cols := make([]Column, ncols)
	for i := 0; i < ncols; i++ {
		cols[i] = newPartitionColumn(c.Field(i), c.Len())
	}
	return StructColumn{cols}
}

// StructColumn:DataColumn

func (c StructColumn) GetItem(rnum int, out []any) {
	for n, c := range c.cols {
		out[n] = c.Value(rnum)
	}
}

func newUnknownColumn(nrows int) UnknownColumn {
	return UnknownColumn{nrows}
}

func (c StructColumn) Item(rnum int) []any {
	row := make([]any, len(c.cols))
	c.GetItem(rnum, row)
	return row
}

func (c StructColumn) NumRows() int {
	if len(c.cols) == 0 {
		return 0
	}
	return c.cols[0].NumRows()
}

func (c StructColumn) String(rnum int) string {
	return "(" + strings.Join(c.Strings(rnum), ", ") + ")"
}

func (c StructColumn) Type() any {
	return AnyListType
}

func (c StructColumn) Value(rnum int) any {
	return c.Item(rnum)
}

// StructColumn:Tabular

func (c StructColumn) Column(rnum int) Column {
	return c.cols[rnum]
}

func (c StructColumn) Columns() []Column {
	return c.cols
}

func (c StructColumn) NumCols() int {
	return len(c.cols)
}

func (c StructColumn) GetRow(rnum int, out []any) {
	for cnum := 0; cnum < len(c.cols); cnum++ {
		out[cnum] = c.cols[cnum].Value(rnum)
	}
}

func (c StructColumn) Row(rnum int) []any {
	result := make([]any, len(c.cols))
	c.GetRow(rnum, result)
	return result
}

func (c StructColumn) Signature() Signature {
	ncols := len(c.cols)
	result := make([]any, ncols)
	for i := 0; i < ncols; i++ {
		result[i] = c.cols[i].Type()
	}
	return result
}

func (c StructColumn) Strings(rnum int) []string {
	ncols := len(c.cols)
	result := make([]string, ncols)
	for cnum := 0; cnum < ncols; cnum++ {
		result[cnum] = c.cols[cnum].String(rnum)
	}
	return result
}

// Represents a column with an unknown data type.
type UnknownColumn struct {
	nrows int
}

func (c UnknownColumn) NumRows() int {
	return c.nrows
}

const unknown = "unknown"

func (c UnknownColumn) GetItem(_ int, out *string) {
	*out = unknown
}

func (c UnknownColumn) Item(_ int) string {
	return unknown
}

func (c UnknownColumn) String(_ int) string {
	return unknown
}

func (c UnknownColumn) Type() any {
	return StringType
}

func (c UnknownColumn) Value(_ int) any {
	return unknown
}

// Returns the native type corresponding to elements of the given arrow array.
func columnType(c arrow.Array) reflect.Type {
	switch cc := c.(type) {
	case *array.Boolean:
		return BoolType
	case *array.Float16:
		return Float16Type
	case *array.Float32:
		return Float32Type
	case *array.Float64:
		return Float64Type
	case *array.Int8:
		return Int8Type
	case *array.Int16:
		return Int16Type
	case *array.Int32:
		return Int32Type
	case *array.Int64:
		return Int64Type
	case *array.String:
		return StringType
	case *array.Uint8:
		return Uint8Type
	case *array.Uint16:
		return Uint16Type
	case *array.Uint32:
		return Uint32Type
	case *array.Uint64:
		return Uint64Type
	case *array.FixedSizeList:
		switch cc.ListValues().(type) {
		case *array.Float32:
			return Float32ListType
		case *array.Float64:
			return Float64ListType
		case *array.Int8:
			return Int8ListType
		case *array.Int16:
			return Int16ListType
		case *array.Int32:
			return Int32ListType
		case *array.Int64:
			return Int64ListType
		case *array.Uint64:
			return Uint64ListType
		case *array.FixedSizeList:
			return Uint64ListType // Rational128
		default:
			return UnknownType
		}
	default:
		// case *array.Struct:
		return reflect.TypeOf(c).Elem()
	}
}

// Partition is the physical representation of relation data. Partitions may
// be shared by relations in the case where they only differ by constant values
// in the relation signature.
func newPartition(record arrow.Record) *Partition {
	return (&Partition{record: record}).init()
}

func (p *Partition) init() *Partition {
	if p.cols == nil {
		ncols := p.NumCols()
		p.cols = make([]Column, ncols)
		for i := 0; i < ncols; i++ {
			p.cols[i] = p.newColumn(i)
		}
	}
	return p
}

func (p *Partition) Record() arrow.Record {
	return p.record
}

// Returns the Arrow schema for the partition.
func (p *Partition) Schema() arrow.Schema {
	return *p.record.Schema()
}

// Partition:Column

func (p *Partition) String(rnum int) string {
	return "(" + strings.Join(p.Strings(rnum), ", ") + ")"
}

func (p *Partition) Type() any {
	return AnyListType
}

func (p *Partition) NumRows() int {
	return int(p.record.NumRows())
}

func (p *Partition) Value(rnum int) any {
	return p.Row(rnum)
}

// Partition:Tabular

func (p *Partition) Column(rnum int) Column {
	return p.cols[rnum]
}

func (p *Partition) Columns() []Column {
	return p.cols
}

func (p *Partition) NumCols() int {
	return int(p.record.NumCols())
}

func (p *Partition) GetRow(rnum int, out []any) {
	ncols := len(p.cols)
	for c := 0; c < ncols; c++ {
		out[c] = p.cols[c].Value(rnum)
	}
}

func (p *Partition) Row(rnum int) []any {
	result := make([]any, len(p.cols))
	p.GetRow(rnum, result)
	return result
}

// Returns the type signature describing the partition.
func (p *Partition) Signature() Signature {
	cols := p.record.Columns()
	result := make(Signature, len(cols))
	for i := 0; i < len(cols); i++ {
		result[i] = columnType(cols[i])
	}
	return result
}

func (p *Partition) Strings(rnum int) []string {
	ncols := len(p.cols)
	row := make([]string, ncols)
	for cnum := 0; cnum < ncols; cnum++ {
		row[cnum] = p.cols[cnum].String(rnum)
	}
	return row
}

// Returns a column accessor for the given partition column index.
func (p *Partition) newColumn(rnum int) Column {
	return newPartitionColumn(p.record.Column(rnum), p.NumRows())
}

// Returns a column accessor for the given arrow array.
func newPartitionColumn(a arrow.Array, nrows int) Column {
	switch aa := a.(type) {
	case *array.Boolean:
		return newBoolColumn(aa)
	case *array.Float16:
		return newFloat16Column(aa.Values())
	case *array.Float32:
		return newFloat32Column(aa.Float32Values())
	case *array.Float64:
		return newFloat64Column(aa.Float64Values())
	case *array.Int8:
		return PrimitiveColumn[int8]{aa.Int8Values()}
	case *array.Int16:
		return PrimitiveColumn[int16]{aa.Int16Values()}
	case *array.Int32:
		return PrimitiveColumn[int32]{aa.Int32Values()}
	case *array.Int64:
		return PrimitiveColumn[int64]{aa.Int64Values()}
	case *array.String:
		return newStringColumn(aa)
	case *array.Uint8:
		return PrimitiveColumn[uint8]{aa.Uint8Values()}
	case *array.Uint16:
		return PrimitiveColumn[uint16]{aa.Uint16Values()}
	case *array.Uint32:
		return PrimitiveColumn[uint32]{aa.Uint32Values()}
	case *array.Uint64:
		return PrimitiveColumn[uint64]{aa.Uint64Values()}
	case *array.FixedSizeList:
		return newListColumn(aa)
	case *array.Struct:
		return newStructColumn(aa)
	}
	return newUnknownColumn(nrows)
}

// Characters are represented in arrow as uint32.
type CharColumn struct {
	col PrimitiveColumn[uint32]
}

func newCharColumn(c PrimitiveColumn[uint32]) CharColumn {
	return CharColumn{c}
}

func (c CharColumn) GetItem(rnum int, out *rune) {
	*out = rune(c.col.data[rnum])
}

func (c CharColumn) Item(rnum int) rune {
	return rune(c.col.data[rnum])
}

func (c CharColumn) NumRows() int {
	return c.col.NumRows()
}

func (c CharColumn) String(rnum int) string {
	return string(rune(c.col.data[rnum]))
}

func (c CharColumn) Type() any {
	return RuneType
}

func (c CharColumn) Value(rnum int) any {
	return rune(c.col.data[rnum])
}

type DateColumn struct {
	col DataColumn[int64]
}

func newDateColumn(col DataColumn[int64]) DateColumn {
	return DateColumn{col}
}

func (c DateColumn) GetItem(rnum int, out *time.Time) {
	*out = c.Item(rnum)
}

func (c DateColumn) Item(rnum int) time.Time {
	v := c.col.Item(rnum) // days since 1AD (Rata Die)
	return DateFromRataDie(v)
}

func (c DateColumn) NumRows() int {
	return c.col.NumRows()
}

func (c DateColumn) String(rnum int) string {
	return c.Item(rnum).Format("2006-01-02")
}

func (c DateColumn) Type() any {
	return TimeType
}

func (c DateColumn) Value(rnum int) any {
	return c.Item(rnum)
}

type DateTimeColumn struct {
	col DataColumn[int64]
}

func newDateTimeColumn(c DataColumn[int64]) DateTimeColumn {
	return DateTimeColumn{c}
}

func (c DateTimeColumn) GetItem(rnum int, out *time.Time) {
	*out = c.Item(rnum)
}

func (c DateTimeColumn) Item(rnum int) time.Time {
	v := c.col.Item(rnum) // millis since 1AD
	return DateFromRataMillis(v)
}

func (c DateTimeColumn) NumRows() int {
	return c.col.NumRows()
}

func (c DateTimeColumn) String(rnum int) string {
	return c.Item(rnum).Format(time.RFC3339)
}

func (c DateTimeColumn) Type() any {
	return TimeType
}

func (c DateTimeColumn) Value(rnum int) any {
	return c.Item(rnum)
}

// DecimalColumn projects the underlying pair of values as a decimal.
type DecimalColumn[T int8 | int16 | int32 | int64] struct {
	col    DataColumn[T]
	digits int32
}

func (c DecimalColumn[T]) NumRows() int {
	return c.col.NumRows()
}

func (c DecimalColumn[T]) Type() any {
	return DecimalType
}

type Decimal8Column struct {
	DecimalColumn[int8]
}

func newDecimal8Column(col DataColumn[int8], digits int32) Decimal8Column {
	return Decimal8Column{DecimalColumn[int8]{col, digits}}
}

func (c Decimal8Column) GetItem(rnum int, out *decimal.Decimal) {
	*out = decimal.New(int64(c.col.Item(rnum)), -c.digits)
}

func (c Decimal8Column) Item(rnum int) decimal.Decimal {
	return decimal.New(int64(c.col.Item(rnum)), -c.digits)
}

func (c Decimal8Column) String(rnum int) string {
	return c.Item(rnum).String()
}

func (c Decimal8Column) Value(rnum int) any {
	return c.Item(rnum)
}

type Decimal16Column struct {
	DecimalColumn[int16]
}

func newDecimal16Column(col DataColumn[int16], digits int32) Decimal16Column {
	return Decimal16Column{DecimalColumn[int16]{col, digits}}
}

func (c Decimal16Column) GetItem(rnum int, out *decimal.Decimal) {
	*out = c.Item(rnum)
}

func (c Decimal16Column) Item(rnum int) decimal.Decimal {
	v := c.col.Item(rnum)
	return decimal.New(int64(v), -c.digits)
}

func (c Decimal16Column) String(rnum int) string {
	return c.Item(rnum).String()
}

func (c Decimal16Column) Value(rnum int) any {
	return c.Item(rnum)
}

type Decimal32Column struct {
	DecimalColumn[int32]
}

func newDecimal32Column(col DataColumn[int32], digits int32) Decimal32Column {
	return Decimal32Column{DecimalColumn[int32]{col, digits}}
}

func (c Decimal32Column) GetItem(rnum int, out *decimal.Decimal) {
	*out = c.Item(rnum)
}

func (c Decimal32Column) Item(rnum int) decimal.Decimal {
	v := c.col.Item(rnum)
	return decimal.New(int64(v), -c.digits)
}

func (c Decimal32Column) String(rnum int) string {
	return c.Item(rnum).String()
}

func (c Decimal32Column) Value(rnum int) any {
	return c.Item(rnum)
}

type Decimal64Column struct {
	DecimalColumn[int64]
}

func newDecimal64Column(col DataColumn[int64], digits int32) Decimal64Column {
	return Decimal64Column{DecimalColumn[int64]{col, digits}}
}

func (c Decimal64Column) GetItem(rnum int, out *decimal.Decimal) {
	*out = c.Item(rnum)
}

func (c Decimal64Column) Item(rnum int) decimal.Decimal {
	v := c.col.Item(rnum)
	return decimal.New(int64(v), -c.digits)
}

func (c Decimal64Column) String(rnum int) string {
	return c.Item(rnum).String()
}

func (c Decimal64Column) Value(rnum int) any {
	return c.Item(rnum)
}

type Decimal128Column struct {
	col    ListColumn[uint64]
	digits int32
}

func newDecimal128Column(col ListColumn[uint64], digits int32) Decimal128Column {
	return Decimal128Column{col, digits}
}

func (c Decimal128Column) GetItem(rnum int, out *decimal.Decimal) {
	*out = c.Item(rnum)
}

func (c Decimal128Column) Item(rnum int) decimal.Decimal {
	var v [2]uint64
	c.col.GetItem(rnum, v[:])
	return NewDecimal128(v[0], v[1], -c.digits)
}

func (c Decimal128Column) NumRows() int {
	return c.col.NumRows()
}

func (c Decimal128Column) String(rnum int) string {
	return c.Item(rnum).String()
}

func (c Decimal128Column) Type() any {
	return DecimalType
}

func (c Decimal128Column) Value(rnum int) any {
	return c.Item(rnum)
}

// ["rel", "base", "FixedDecimal", <bits>, <digits>]
func newDecimalColumn(vt ValueType, c Column) Column {
	digits := int32(vt[4].(int64))
	switch vt[3].(int64) {
	case 8:
		return newDecimal8Column(c.(DataColumn[int8]), digits)
	case 16:
		return newDecimal16Column(c.(DataColumn[int16]), digits)
	case 32:
		return newDecimal32Column(c.(DataColumn[int32]), digits)
	case 64:
		return newDecimal64Column(c.(DataColumn[int64]), digits)
	case 128:
		return newDecimal128Column(c.(ListColumn[uint64]), digits)
	}
	return newUnknownColumn(c.NumRows())
}

// Int128Column projects the underlying `[2]int64“ value as a `big.Int`.
type Int128Column struct {
	col ListColumn[uint64]
}

func newInt128Column(c ListColumn[uint64]) Int128Column {
	return Int128Column{c}
}

func (c Int128Column) GetItem(rnum int, out **big.Int) {
	*out = c.Item(rnum)
}

func (c Int128Column) Item(rnum int) *big.Int {
	v := c.col.Item(rnum)
	// assert len(v) == 2
	return NewBigInt128(v[0], v[1])
}

func (c Int128Column) NumRows() int {
	return c.col.NumRows()
}

func (c Int128Column) String(rnum int) string {
	return c.Item(rnum).String()
}

func (c Int128Column) Type() any {
	return BigIntType
}

func (c Int128Column) Value(rnum int) any {
	return c.Item(rnum)
}

type LiteralColumn[T any] struct {
	value T
	nrows int
}

func newLiteralColumn[T any](v T, nrows int) LiteralColumn[T] {
	return LiteralColumn[T]{v, nrows}
}

func (c LiteralColumn[T]) GetItem(rnum int, out *T) {
	*out = c.value
}

func (c LiteralColumn[T]) Item(rnum int) T {
	return c.value
}

func (c LiteralColumn[T]) NumRows() int {
	return c.nrows
}

func (c LiteralColumn[T]) String(_ int) string {
	return asString(c.value)
}

func (c LiteralColumn[T]) Type() any {
	return reflect.TypeOf(c.value)
}

func (c LiteralColumn[T]) Value(_ int) any {
	return c.value
}

// Uint128Column projects the underlying `[2]uint64“ value as `big.Int`.
type Uint128Column struct {
	col ListColumn[uint64]
}

func newUint128Column(c ListColumn[uint64]) Uint128Column {
	return Uint128Column{c}
}

func (c Uint128Column) GetItem(rnum int, out **big.Int) {
	*out = c.Item(rnum)
}

func (c Uint128Column) Item(rnum int) *big.Int {
	var v [2]uint64
	c.col.GetItem(rnum, v[:])
	return NewBigUint128(v[0], v[1])
}

func (c Uint128Column) NumRows() int {
	return c.col.NumRows()
}

func (c Uint128Column) String(rnum int) string {
	return c.Item(rnum).String()
}

func (c Uint128Column) Type() any {
	return BigIntType
}

func (c Uint128Column) Value(rnum int) any {
	return c.Item(rnum)
}

// RationalColumn projects the underlying pair of values as a `*big.Rat“.
type RationalColumn[T int8 | int16 | int32 | int64] struct {
	col ListColumn[T]
}

func newRational8Column(col ListColumn[int8]) Rational8Column {
	return Rational8Column{RationalColumn[int8]{col}}
}

func (c RationalColumn[T]) NumRows() int {
	return c.col.NumRows()
}

func (c RationalColumn[T]) Type() any {
	return RationalType
}

type Rational8Column struct {
	RationalColumn[int8]
}

func (c Rational8Column) GetItem(rnum int, out **big.Rat) {
	*out = c.Item(rnum)
}

func (c Rational8Column) Item(rnum int) *big.Rat {
	var v [2]int8
	c.col.GetItem(rnum, v[:])
	n, d := int64(v[0]), int64(v[1])
	return big.NewRat(n, d)
}

func (c Rational8Column) String(rnum int) string {
	return c.Item(rnum).String()
}

func (c Rational8Column) Value(rnum int) any {
	return c.Item(rnum)
}

type Rational16Column struct {
	RationalColumn[int16]
}

func newRational16Column(col ListColumn[int16]) Rational16Column {
	return Rational16Column{RationalColumn[int16]{col}}
}

func (c Rational16Column) GetItem(rnum int, out **big.Rat) {
	*out = c.Item(rnum)
}

func (c Rational16Column) Item(rnum int) *big.Rat {
	var v [2]int16
	c.col.GetItem(rnum, v[:])
	n, d := int64(v[0]), int64(v[1])
	return big.NewRat(n, d)
}

func (c Rational16Column) String(rnum int) string {
	return c.Item(rnum).String()
}

func (c Rational16Column) Value(rnum int) any {
	return c.Item(rnum)
}

type Rational32Column struct {
	RationalColumn[int32]
}

func newRational32Column(col ListColumn[int32]) Rational32Column {
	return Rational32Column{RationalColumn[int32]{col}}
}

func (c Rational32Column) GetItem(rnum int, out **big.Rat) {
	*out = c.Item(rnum)
}

func (c Rational32Column) Item(rnum int) *big.Rat {
	var v [2]int32
	c.col.GetItem(rnum, v[:])
	n, d := int64(v[0]), int64(v[1])
	return big.NewRat(n, d)
}

func (c Rational32Column) String(rnum int) string {
	return c.Item(rnum).String()
}

func (c Rational32Column) Value(rnum int) any {
	return c.Item(rnum)
}

type Rational64Column struct {
	RationalColumn[int64]
}

func newRational64Column(col ListColumn[int64]) Rational64Column {
	return Rational64Column{RationalColumn[int64]{col}}
}

func (c Rational64Column) GetItem(rnum int, out **big.Rat) {
	*out = c.Item(rnum)
}

func (c Rational64Column) Item(rnum int) *big.Rat {
	var v [2]int64
	c.col.GetItem(rnum, v[:])
	return big.NewRat(v[0], v[1])
}

func (c Rational64Column) String(rnum int) string {
	return c.Item(rnum).String()
}

func (c Rational64Column) Value(rnum int) any {
	return c.Item(rnum)
}

type Rational128Column struct {
	col ListColumn[uint64]
}

func newRational128Column(col ListColumn[uint64]) Rational128Column {
	return Rational128Column{col}
}

func (c Rational128Column) GetItem(rnum int, out **big.Rat) {
	*out = c.Item(rnum)
}

func (c Rational128Column) NumRows() int {
	return c.col.NumRows()
}

func (c Rational128Column) Item(rnum int) *big.Rat {
	var v [4]uint64
	c.col.GetItem(rnum, v[:])
	n := NewBigInt128(v[0], v[1])
	d := NewBigInt128(v[2], v[3])
	return NewRational128(n, d)
}

func (c Rational128Column) String(rnum int) string {
	return c.Item(rnum).String()
}

func (c Rational128Column) Type() any {
	return RationalType
}

func (c Rational128Column) Value(rnum int) any {
	return c.Item(rnum)
}

func newRationalColumn(c Column) Column {
	switch cc := c.(type) {
	case ListColumn[int8]:
		return newRational8Column(cc)
	case ListColumn[int16]:
		return newRational16Column(cc)
	case ListColumn[int32]:
		return newRational32Column(cc)
	case ListColumn[int64]:
		return newRational64Column(cc)
	case ListColumn[uint64]:
		return newRational128Column(cc)
	}
	return newUnknownColumn(c.NumRows())
}

type SymbolColumn struct {
	value string
	nrows int
}

func newSymbolColumn(v string, nrows int) SymbolColumn {
	return SymbolColumn{v, nrows}
}

func (c SymbolColumn) GetItem(_ int, out *string) {
	*out = c.value
}

func (c SymbolColumn) Item(_ int) string {
	return c.value
}

func (c SymbolColumn) NumRows() int {
	return c.nrows
}

func (c SymbolColumn) String(_ int) string {
	return c.value
}

func (c SymbolColumn) Type() any {
	return StringType
}

func (c SymbolColumn) Value(_ int) any {
	return c.value
}

const missing = "missing"

type MissingColumn struct {
	nrows int
}

func newMissingColumn(nrows int) MissingColumn {
	return MissingColumn{nrows}
}

func (c MissingColumn) GetItem(_ int, out *any) {
	*out = missing
}

func (c MissingColumn) Item(_ int) any {
	return missing
}

func (c MissingColumn) NumRows() int {
	return c.nrows
}

func (c MissingColumn) String(_ int) string {
	return missing
}

func (c MissingColumn) Type() any {
	return MissingType
}

func (c MissingColumn) Value(_ int) any {
	return missing
}

// ["rel", "base", "Decimal", <bits>, <digits>, <value>]
func newConstDecimalColumn(t ConstType, nrows int) Column {
	var d decimal.Decimal
	digits := int32(t[4].(int64))
	switch t[3].(int64) {
	case 8:
		d = decimal.New(int64(t[5].(int8)), -digits)
	case 16:
		d = decimal.New(int64(t[5].(int16)), -digits)
	case 32:
		d = decimal.New(int64(t[5].(int32)), -digits)
	case 64:
		d = decimal.New(t[5].(int64), -digits)
	case 128:
		d = decimal.NewFromBigInt(t[5].(*big.Int), -digits)
	default:
		return newUnknownColumn(nrows)
	}
	return newLiteralColumn(d, nrows)
}

// ["rel", "base", "Rational", <bits>, <num>, <denom>]
func newConstRationalColumn(t ConstType, nrows int) Column {
	var r *big.Rat
	switch t[3].(int64) {
	case 8:
		n, d := t[4].(int8), t[5].(int8)
		r = big.NewRat(int64(n), int64(d))
	case 16:
		n, d := t[4].(int16), t[5].(int16)
		r = big.NewRat(int64(n), int64(d))
	case 32:
		n, d := t[4].(int32), t[5].(int32)
		r = big.NewRat(int64(n), int64(d))
	case 64:
		n, d := t[4].(int64), t[5].(int64)
		r = big.NewRat(int64(n), int64(d))
	case 128:
		n, d := t[4].(*big.Int), t[5].(*big.Int)
		r = NewRational128(n, d)
	default:
		return newUnknownColumn(nrows)
	}
	return newLiteralColumn(r, nrows)
}

type ConstColumn struct {
	cols  []Column
	nrows int
	vals  []any
}

// ConstColumn:DataColumn

func (c ConstColumn) GetItem(rnum int, out []any) {
	for cnum := 0; cnum < len(out); cnum++ {
		out[cnum] = c.cols[cnum].Value(rnum)
	}
}

func (c ConstColumn) Item(_ int) []any {
	if c.vals == nil {
		c.vals = make([]any, len(c.cols))
		c.GetItem(0, c.vals)
	}
	return c.vals
}

func (c ConstColumn) NumRows() int {
	return c.nrows
}

func (c ConstColumn) String(rnum int) string {
	return "(" + strings.Join(c.Strings(rnum), ", ") + ")"
}

func (c ConstColumn) Type() any {
	return AnyListType
}

func (c ConstColumn) Value(rnum int) any {
	return c.Item(rnum)
}

// ConstColumn:Tabular

func (c ConstColumn) Column(cnum int) Column {
	return c.cols[cnum]
}

func (c ConstColumn) Columns() []Column {
	return c.cols
}

func (c ConstColumn) NumCols() int {
	return len(c.cols)
}

func (c ConstColumn) GetRow(rnum int, out []any) {
	c.GetItem(rnum, out)
}

func (c ConstColumn) Row(rnum int) []any {
	return c.Item(rnum)
}

func (c ConstColumn) Signature() Signature {
	ncols := len(c.cols)
	result := make([]any, ncols)
	for i := 0; i < ncols; i++ {
		result[i] = c.cols[i].Type()
	}
	return result
}

func (c ConstColumn) Strings(rnum int) []string {
	ncols := len(c.cols)
	result := make([]string, ncols)
	for cnum := 0; cnum < ncols; cnum++ {
		result[cnum] = c.cols[cnum].String(rnum)
	}
	return result
}

func newConstColumn(t ConstType, nrows int) Column {
	if matchPrefix(t, "rel", "base", "_") {
		switch t[2].(string) {
		case "AutoNumber":
			return newLiteralColumn(t[3].(int64), nrows)
		case "Date":
			d := DateFromRataDie(t[3].(int64))
			return newLiteralColumn(d, nrows)
		case "DateTime":
			d := DateFromRataMillis(t[3].(int64))
			return newLiteralColumn(d, nrows)
		case "FilePos":
			return newLiteralColumn(t[3].(int64), nrows)
		case "FixedDecimal":
			return newConstDecimalColumn(t, nrows)
		case "Hash":
			return newLiteralColumn(t[3].(*big.Int), nrows)
		case "Rational":
			return newConstRationalColumn(t, nrows)
		case "Missing":
			return newMissingColumn(nrows)
		case "Year", "Month", "Week", "Day", "Hour", "Minute",
			"Second", "Millisecond", "Microsecond", "Nanosecond":
			return newLiteralColumn(t[3].(int64), nrows)
		}
	}
	cols := make([]Column, len(t))
	for i, v := range t {
		var cc Column
		switch tt := v.(type) {
		case ConstType:
			cc = newConstColumn(tt, nrows)
		case ValueType: // unexpected
			cc = newUnknownColumn(nrows)
		default:
			cc = newLiteralColumn(v, nrows)
		}
		cols[i] = cc
	}
	return ConstColumn{cols, nrows, nil}
}

type ValueColumn struct {
	cols []Column
}

func newBuiltinColumn(vt ValueType, c Column, nrows int) Column {
	if matchPrefix(vt, "rel", "base", "_") {
		switch vt[2].(string) {
		case "AutoNumber":
			return c // PrimitiveColumn[int64]
		case "Date":
			return newDateColumn(c.(DataColumn[int64]))
		case "DateTime":
			return newDateTimeColumn(c.(DataColumn[int64]))
		case "FilePos":
			return c // PrimitiveColumn[int64]
		case "FixedDecimal":
			return newDecimalColumn(vt, c)
		case "Hash":
			return newUint128Column(c.(ListColumn[uint64]))
		case "Rational":
			return newRationalColumn(c)
		case "Missing":
			return newMissingColumn(nrows)
		case "Year", "Month", "Week", "Day", "Hour", "Minute",
			"Second", "Millisecond", "Microsecond", "Nanosecond":
			return c
		}
	}
	return nil // not a recognized builtin value type
}

// Projects a ValueColumn from an underlying simple column.
func newSimpleValueColumn(vt ValueType, c Column, nrows int) ValueColumn {
	ncols := len(vt)
	cols := make([]Column, ncols)
	for i, t := range vt {
		var cc Column
		switch tt := t.(type) {
		case ValueType: // todo: this should implement tabular
			cc = newValueColumn(tt, c, nrows)
		default:
			cc = newRelationColumn(tt, c, nrows)
		}
		cols[i] = cc
	}
	return ValueColumn{cols}
}

// Projects a ValueColumn from an underlying `Tabular` column.
func newTabularValueColumn(vt ValueType, c Tabular, nrows int) ValueColumn {
	ncol := 0
	ncols := len(vt)
	cols := make([]Column, ncols)
	for i, t := range vt {
		var cc Column
		switch tt := t.(type) {
		case reflect.Type:
			cc = newRelationColumn(tt, c.Column(ncol), nrows)
			ncol++
		case ValueType:
			cc = newValueColumn(tt, c.Column(ncol), nrows)
			ncol++
		case string:
			cc = newSymbolColumn(tt, nrows)
		default:
			cc = newLiteralColumn(tt, nrows)
		}
		cols[i] = cc
	}
	return ValueColumn{cols}
}

// Returns a ValueColumn which is a projection of the given partition column.
func newValueColumn(vt ValueType, c Column, nrows int) Column {
	if cc := newBuiltinColumn(vt, c, nrows); cc != nil {
		return cc
	}
	if cc, ok := c.(Tabular); ok {
		return newTabularValueColumn(vt, cc, nrows)
	}
	return newSimpleValueColumn(vt, c, nrows)
}

// ValueColumn:DataColumn

func (c ValueColumn) GetItem(rnum int, out []any) {
	for cnum := 0; cnum < len(out); cnum++ {
		out[cnum] = c.cols[cnum].Value(rnum)
	}
}

func (c ValueColumn) Item(rnum int) []any {
	result := make([]any, len(c.cols))
	c.GetItem(rnum, result)
	return result
}

func (c ValueColumn) NumRows() int {
	return c.cols[0].NumRows()
}

func (c ValueColumn) String(rnum int) string {
	return "(" + strings.Join(c.Strings(rnum), ", ") + ")"
}

func (c ValueColumn) Type() any {
	return AnyListType
}

func (c ValueColumn) Value(rnum int) any {
	return c.Item(rnum)
}

// ValueColumn:Tabular

func (c ValueColumn) Column(cnum int) Column {
	return c.cols[cnum]
}

func (c ValueColumn) Columns() []Column {
	return c.cols
}

func (c ValueColumn) NumCols() int {
	return len(c.cols)
}

func (c ValueColumn) GetRow(rnum int, out []any) {
	c.GetItem(rnum, out)
}

func (c ValueColumn) Row(rnum int) []any {
	return c.Item(rnum)
}

func (c ValueColumn) Signature() Signature {
	ncols := len(c.cols)
	result := make([]any, ncols)
	for i := 0; i < ncols; i++ {
		result[i] = c.cols[i].Type()
	}
	return result
}

func (c ValueColumn) Strings(rnum int) []string {
	ncols := len(c.cols)
	result := make([]string, ncols)
	for cnum := 0; cnum < ncols; cnum++ {
		result[cnum] = c.cols[cnum].String(rnum)
	}
	return result
}

type Relation interface {
	Tabular
	Slice(int, ...int) Relation
}

type baseRelation struct {
	meta  Signature
	part  *Partition
	sig   Signature
	cols  []Column
	nrows int
}

// Initialize row count and instantiate relation columns.
func (r *baseRelation) init() *baseRelation {
	if r.cols != nil {
		return r
	}

	// place partition columns in position coresponding to the metadata
	ncols := 0 // count of arrow columns consumed (there can be empty extras)
	pcols := make([]Column, len(r.meta))
	for i, m := range r.meta {
		if !isConstType(m) {
			pcols[i] = r.part.Column(ncols)
			ncols++
		}
	}

	// If the relation is fully specialized, the row count is 1, and there
	// will be no arrow data, otherwise the row count is determined by the
	// number of rows of arrow data.
	if ncols == 0 {
		r.nrows = 1
	} else {
		r.nrows = r.part.NumRows()
	}

	r.cols = make([]Column, len(r.meta))
	for i, m := range r.meta {
		c := newRelationColumn(m, pcols[i], r.nrows)
		r.cols[i] = c
	}

	return r
}

func newBaseRelation(p *Partition, meta Signature) Relation {
	return (&baseRelation{part: p, meta: meta}).init()
}

func (r *baseRelation) Partition() *Partition {
	return r.part
}

// baseRelation:Column

func (r *baseRelation) NumRows() int {
	return r.nrows
}

func (r *baseRelation) String(rnum int) string {
	return "(" + strings.Join(r.Strings(rnum), ", ") + ")"
}

func (r *baseRelation) Type() any {
	return AnyListType
}

func (r *baseRelation) Value(rnum int) any {
	return r.Row(rnum)
}

// baseRelation:Tabular

// Ensure the relations type signature is instantiated.
func (r *baseRelation) ensureSignature() Signature {
	if r.sig != nil {
		return r.sig
	}
	r.sig = make([]any, len(r.meta))
	for i, t := range r.meta {
		r.sig[i] = relationType(t)
	}
	return r.sig
}

func (r *baseRelation) Column(cnum int) Column {
	return r.cols[cnum]
}

func (r *baseRelation) Columns() []Column {
	return r.cols
}

func (r *baseRelation) NumCols() int {
	return len(r.meta)
}

func (r *baseRelation) GetRow(rnum int, out []any) {
	for cnum := 0; cnum < len(r.cols); cnum++ {
		out[cnum] = r.cols[cnum].Value(rnum)
	}
}

func (r *baseRelation) Row(rnum int) []any {
	result := make([]any, len(r.cols))
	r.GetRow(rnum, result)
	return result
}

func (r *baseRelation) Signature() Signature {
	return r.ensureSignature()
}

func (r *baseRelation) Strings(rnum int) []string {
	ncols := len(r.cols)
	result := make([]string, ncols)
	for cnum := 0; cnum < ncols; cnum++ {
		result[cnum] = r.cols[cnum].String(rnum)
	}
	return result
}

// Answers if the given type describes a constant value.
func isConstType(t any) bool {
	switch t.(type) {
	case reflect.Type, ValueType:
		return false
	}
	return true
}

// Answers if the given type is a primitive type that may appear in a relation.
func isRelationPrimitive(t reflect.Type) bool {
	switch t {
	case BoolType:
		return true
	case Float16Type, Float32Type, Float64Type:
		return true
	case Int8Type, Int16Type, Int32Type, Int64Type:
		return true
	case Uint8Type, Uint16Type, Uint32Type, Uint64Type:
		return true
	case StringType:
		return true
	}
	return false
}

func builtinType(t []any) reflect.Type {
	if matchPrefix(t, "rel", "base", "_") {
		switch t[2].(string) {
		case "AutoNumber":
			return Int64Type
		case "Date":
			return TimeType
		case "DateTime":
			return TimeType
		case "FixedDecimal":
			return DecimalType
		case "FilePos":
			return Int64Type
		case "Hash":
			return BigIntType
		case "Missing":
			return MissingType
		case "Rational":
			return RationalType
		case "Year", "Month", "Week", "Day", "Hour", "Minute",
			"Second", "Millisecond", "Microsecond", "Nanosecond":
			return Int64Type
		}
	}
	return nil
}

// Maps the given metadata element to the corresponding relation type.
func relationType(t any) any {
	switch tt := t.(type) {
	case reflect.Type: // primitive type
		switch tt {
		case CharType:
			return RuneType
		case Int128Type, Uint128Type:
			return BigIntType
		default:
			return tt
		}
	case ConstType:
		if bt := builtinType(tt); bt != nil {
			return bt
		}
		result := make(ConstType, len(tt))
		for i, t := range tt {
			result[i] = relationType(t)
		}
		return result
	case ValueType:
		if bt := builtinType(tt); bt != nil {
			return bt
		}
		result := make(ValueType, len(tt))
		for i, t := range tt {
			result[i] = relationType(t)
		}
		return result
	case string: // constant string, aka symbol
		return StringType
	default: // constant value other than string
		return reflect.TypeOf(tt)
	}
}

func newRelationColumn(t any, col Column, nrows int) Column {
	switch tt := t.(type) {
	case reflect.Type:
		switch tt {
		case CharType:
			return newCharColumn(col.(PrimitiveColumn[uint32]))
		case Int128Type:
			return newInt128Column(col.(ListColumn[uint64]))
		case Uint128Type:
			return newUint128Column(col.(ListColumn[uint64]))
		default:
			if isRelationPrimitive(tt) {
				return col // passed through
			}
			return newUnknownColumn(nrows)
		}
	case ConstType:
		return newConstColumn(tt, nrows)
	case ValueType:
		return newValueColumn(tt, col, nrows)
	case string:
		return newSymbolColumn(tt, nrows)
	default: // constant value other than string
		return newLiteralColumn(tt, nrows)
	}
}

func (r baseRelation) Slice(lo int, hi ...int) Relation {
	var c []Column
	if len(hi) > 0 {
		c = r.cols[lo:hi[0]]
	} else {
		c = r.cols[lo:]
	}
	return newDerivedRelation(c)
}

// Introduced when relations of different arity are unioned.
type NilColumn struct {
	nrows int
}

func newNilColumn(nrows int) NilColumn {
	return NilColumn{nrows}
}

func (c NilColumn) GetItem(_ int, out *any) {
	*out = nil
}

func (c NilColumn) Item(_ int) any {
	return nil
}

func (c NilColumn) NumRows() int {
	return c.nrows
}

func (c NilColumn) String(_ int) string {
	return "<nil>"
}

func (c NilColumn) Type() any {
	return reflect.TypeOf(nil)
}

func (c NilColumn) Value(_ int) any {
	return nil
}

// Unions the  given columns into a single column.
type unionColumn struct {
	cols    []Column
	nrows   int
	colType any
}

func (c unionColumn) init() unionColumn {
	c.nrows = c.cols[0].NumRows()
	c.colType = c.cols[0].Type()
	for _, cc := range c.cols[1:] {
		c.nrows += cc.NumRows()
		if c.colType != cc.Type() {
			c.colType = MixedType
		}
	}
	return c
}

func newUnionColumn(cols []Column) unionColumn {
	return (unionColumn{cols, -1, nil}).init()
}

func (c unionColumn) NumRows() int {
	return c.nrows
}

func (c unionColumn) String(rnum int) string {
	for _, cc := range c.cols {
		nrows := cc.NumRows()
		if rnum < nrows {
			return cc.String(rnum)
		}
		rnum -= nrows
	}
	return "" // rnum out of range
}

func (c unionColumn) Type() any {
	return c.colType
}

func (c unionColumn) Value(rnum int) any {
	for _, cc := range c.cols {
		nrows := cc.NumRows()
		if rnum < nrows {
			return cc.Value(rnum)
		}
		rnum -= nrows
	}
	return nil // rnum out of range
}

// Returns the maximum number of colums in the given list or relations.
func maxNumCols(rs []Relation) int {
	max := 0
	for _, r := range rs {
		ncols := r.NumCols()
		if max < ncols {
			max = ncols
		}
	}
	return max
}

func makeUnionColumn(rels []Relation, cnum int) unionColumn {
	cols := make([]Column, len(rels))
	for i, r := range rels {
		if cnum < r.NumCols() {
			cols[i] = r.Column(cnum)
		} else {
			cols[i] = newNilColumn(r.NumRows())
		}
	}
	return newUnionColumn(cols)
}

func newUnionRelation(rs []Relation) Relation {
	if len(rs) == 1 {
		return rs[0]
	}
	ncols := maxNumCols(rs)
	cols := make([]Column, ncols)
	for cnum := 0; cnum < ncols; cnum++ {
		cols[cnum] = makeUnionColumn(rs, cnum)
	}
	return newDerivedRelation(cols)
}

//
// derivedRealtion
//

type derivedRelation struct {
	cols []Column
}

// derivedRelation:Column

func (r derivedRelation) NumRows() int {
	return r.cols[0].NumRows()
}

func (r derivedRelation) String(rnum int) string {
	return "(" + strings.Join(r.Strings(rnum), ", ") + ")"
}

func (r derivedRelation) Type() any {
	return AnyListType
}

func (r derivedRelation) Value(rnum int) any {
	return r.Row(rnum)
}

// derivedRelation:Tabular

func (r derivedRelation) Column(cnum int) Column {
	return r.cols[cnum]
}

func (r derivedRelation) Columns() []Column {
	return r.cols
}

func (r derivedRelation) NumCols() int {
	return len(r.cols)
}

func (r derivedRelation) GetRow(rnum int, out []any) {
	for cnum, c := range r.cols {
		out[cnum] = c.Value(rnum)
	}
}

func (r derivedRelation) Row(rnum int) []any {
	result := make([]any, len(r.cols))
	r.GetRow(rnum, result)
	return result
}

func (r derivedRelation) Signature() Signature {
	ncols := len(r.cols)
	result := make([]any, ncols)
	for i := 0; i < ncols; i++ {
		result[i] = r.cols[i].Type()
	}
	return result
}

func (r derivedRelation) Slice(lo int, hi ...int) Relation {
	var c []Column
	if len(hi) > 0 {
		c = r.cols[lo:hi[0]]
	} else {
		c = r.cols[lo:]
	}
	return newDerivedRelation(c)
}

func (r derivedRelation) Strings(rnum int) []string {
	ncols := len(r.cols)
	result := make([]string, ncols)
	for cnum := 0; cnum < ncols; cnum++ {
		result[cnum] = r.cols[cnum].String(rnum)
	}
	return result
}

func newDerivedRelation(cols []Column) Relation {
	return derivedRelation{cols}
}

//
// RelationCollection
//

type RelationCollection []Relation

func (c RelationCollection) Select(args ...any) RelationCollection {
	if len(args) == 0 {
		return c
	}
	pre := Signature(args)
	rs := []Relation{}
	for _, r := range c {
		sig := r.Signature()
		if matchSig(pre, sig) {
			rs = append(rs, r)
		}
	}
	return RelationCollection(rs)
}

func (c RelationCollection) Union() Relation {
	return newUnionRelation(c)
}

//
// TransactionResponse
//

func (t *TransactionResponse) EnsureMetadata(c *Client) (*TransactionMetadata, error) {
	if t.Metadata == nil {
		metadata, err := c.GetTransactionMetadata(t.Transaction.ID)
		if err != nil {
			return nil, err
		}
		t.Metadata = metadata
	}
	return t.Metadata, nil
}

func (t *TransactionResponse) EnsureProblems(c *Client) ([]Problem, error) {
	if t.Problems == nil {
		problems, err := c.GetTransactionProblems(t.Transaction.ID)
		if err != nil {
			return nil, err
		}
		t.Problems = problems
	}
	return t.Problems, nil
}

func (t *TransactionResponse) EnsureResults(c *Client) (map[string]*Partition, error) {
	if t.Partitions == nil {
		partitions, err := c.GetTransactionResults(t.Transaction.ID)
		if err != nil {
			return nil, err
		}
		t.Partitions = partitions
	}
	return t.Partitions, nil
}

func (t *TransactionResponse) Partition(id string) *Partition {
	return t.Partitions[id]
}

func (t *TransactionResponse) Relation(id string) Relation {
	return newBaseRelation(t.Partitions[id], t.Signature(id))
}

// Answers if the given signature prefix matches the given signature, where
// the value "_" is a position wildcard.
func matchSig(pre, sig Signature) bool {
	if pre == nil {
		return true
	}
	if len(pre) > len(sig) {
		return false
	}
	for i, p := range pre {
		if p == "_" {
			continue
		}
		if p != sig[i] {
			return false
		}
	}
	return true
}

// Returns a collection of relations whose signature matches any of the
// optional prefix arguments, where value "_" in the prefix matches any value in the
// corresponding signature position.
func (t *TransactionResponse) Relations(args ...any) RelationCollection {
	if t.Metadata == nil {
		// cannot interpret partition data as without metadata
		return RelationCollection{}
	}
	if t.relations == nil {
		// construct collection of base relations
		c := RelationCollection{}
		for id, p := range t.Partitions {
			c = append(c, newBaseRelation(p, t.Signature(id)))
		}
		t.relations = c
	}
	return t.relations.Select(args...)
}

// Returns the type signature corresponding to the given relation ID.
func (t TransactionResponse) Signature(id string) Signature {
	return t.Metadata.Signature(id)
}
