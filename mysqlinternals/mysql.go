// sqlinternals for github.com/go-sql-driver/mysql - retrieve column metadata from sql.*Row / sql.*Rows
//
// Copyright 2013 Arne Hormann. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysqlinternals

import (
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"time"

	"github.com/go-sql-driver/mysql"
)

// Column represents the column of a MySQL result.
// The methods below postfixed with (*) return information for MySQL internal flags.
// Please note that I can't say if these are trustworthy (esp. IsNotNull), they come directly from MySQL.
// At least for SCHEMA information, MySQL can report false metadata, I don't know if this is different for results.
type Column interface {

	// mysql.name

	// Name returns the column name, matching that of a call to Columns() in database/sql
	Name() string

	// derived from mysqlField.fieldType

	// MysqlType returns the raw sql type name without parameters and modifiers
	MysqlType() string
	// IsNumber returns true if the column contains numbers (one of integer, decimal or floating point)
	IsNumber() bool
	// IsInteger returns true if the column contains integers
	IsInteger() bool
	// IsFloatingPoint returns true if the column contains floating point numbers
	IsFloatingPoint() bool
	// IsDecimal returns true if the column contains decimal numbers
	IsDecimal() bool
	// IsText returns true if the column contains textual data
	IsText() bool
	// IsBlob returns true if the column contains binary blobs
	IsBlob() bool
	// IsTime returns true if the column contains temporal data
	IsTime() bool

	// derived from mysqlField.flags
	// TODO: not quite sure about these, add tests and check them.

	// IsPrimaryKey returns true if the column is marked as part of a primary key (*).
	IsPrimaryKey() bool
	// IsUniqueKey returns true if the column is marked as part of a unique key (*).
	IsUniqueKey() bool
	// IsMultipleKey returns true if the column is marked as part of a regular key (*).
	IsMultipleKey() bool
	// IsNotNull returns true if the column is marked as NOT NULL (*).
	IsNotNull() bool
	// IsUnsigned returns true if the column is marked as UNSIGNED (*).
	IsUnsigned() bool
	// IsZerofill returns true if the column is marked as ZEROFILL (*).
	IsZerofill() bool
	// IsBinary returns true if the column is marked as BINARY (*).
	IsBinary() bool
	// IsAutoIncrement returns true if the column is marked as AUTO_INCREMENT (*).
	IsAutoIncrement() bool

	// derived from mysqlField.decimals
	Decimals() int

	// derived from mysqlField.fieldType and mysqlField.flags

	// MysqlParameters returns the category of parameters the SQL type expects in MysqlDeclaration.
	MysqlParameters() parameterType
	// MysqlDeclaration returns a type declaration usable in a CREATE TABLE statement.
	MysqlDeclaration(params ...interface{}) (string, error)
	// ReflectGoType returns the smallest Go type able to represent all possible regular values.
	// The returned types assume a non-NULL value and may cause problems
	// on conversion (e.g. MySQL DATE "0000-00-00", which is not mappable to Go).
	ReflectGoType() (reflect.Type, error)
	// ReflectSqlType returns a Go type able to contain the SQL type, including null values.
	// The returned types may cause problems on conversion
	// (e.g. MySQL DATE "0000-00-00", which is not mappable to Go).
	// The returned type assumes IsNotNull() to be false when forceNullable is set
	// and attempts to return a nullable type (e.g. sql.NullString instead of string).
	ReflectSqlType(forceNullable bool) (reflect.Type, error)
}

var _ Column = mysqlField{}

// name of the column
func (f mysqlField) Name() string {
	return f.name
}

// is a numeric type
func (f mysqlField) IsNumber() bool {
	return f.IsInteger() || f.IsFloatingPoint() || f.IsDecimal()
}

// is a numeric integer type
func (f mysqlField) IsInteger() bool {
	switch f.fieldType {
	case fieldTypeTiny, fieldTypeShort, fieldTypeInt24, fieldTypeLong, fieldTypeLongLong:
		return true
	}
	return false
}

// is a numeric binary floating point type
func (f mysqlField) IsFloatingPoint() bool {
	switch f.fieldType {
	case fieldTypeFloat, fieldTypeDouble:
		return true
	}
	return false
}

// is a numeric decimal type
func (f mysqlField) IsDecimal() bool {
	switch f.fieldType {
	case fieldTypeDecimal, fieldTypeNewDecimal:
		return true
	}
	return false
}

// is a blob type
func (f mysqlField) IsBlob() bool {
	switch f.fieldType {
	case fieldTypeTinyBLOB, fieldTypeMediumBLOB, fieldTypeBLOB, fieldTypeLongBLOB:
		return true
	}
	return false
}

// is a textual type
func (f mysqlField) IsText() bool {
	switch f.fieldType {
	case fieldTypeVarChar, fieldTypeVarString, fieldTypeString:
		return true
	}
	return false
}

// is a temporal type
func (f mysqlField) IsTime() bool {
	switch f.fieldType {
	case fieldTypeYear, fieldTypeDate, fieldTypeNewDate, fieldTypeTime, fieldTypeTimestamp, fieldTypeDateTime:
		return true
	}
	return false
}

// type name in MySQL (includes "NULL", which may not be used in table definitions)
func (f mysqlField) MysqlType() string {
	return mysqlNameFor(f.fieldType)
}

// is part of the primary key
func (f mysqlField) IsPrimaryKey() bool {
	return f.flags&flagPriKey == flagPriKey
}

// is part of a unique key
func (f mysqlField) IsUniqueKey() bool {
	return f.flags&flagUniqueKey == flagUniqueKey
}

// is part of a nonunique key
func (f mysqlField) IsMultipleKey() bool {
	return f.flags&flagMultipleKey == flagMultipleKey
}

// has NOT NULL attribute set
func (f mysqlField) IsNotNull() bool {
	return f.flags&flagNotNULL == flagNotNULL
}

// has UNSIGNED attribute set
func (f mysqlField) IsUnsigned() bool {
	return f.flags&flagUnsigned == flagUnsigned
}

// has ZEROFILL attribute set
func (f mysqlField) IsZerofill() bool {
	return f.flags&flagZeroFill == flagZeroFill
}

// has BINARY attribute set
func (f mysqlField) IsBinary() bool {
	return f.flags&flagBinary == flagBinary
}

// has AUTO_INCREMENT attribute set
func (f mysqlField) IsAutoIncrement() bool {
	return f.flags&flagAutoIncrement == flagAutoIncrement
}

func (f mysqlField) Decimals() int {
	return int(f.decimals)
}

const ( // base for reflection
	reflect_uint8   = uint8(0)
	reflect_uint16  = uint16(0)
	reflect_uint32  = uint32(0)
	reflect_uint64  = uint64(0)
	reflect_int8    = int8(0)
	reflect_int16   = int16(0)
	reflect_int32   = int32(0)
	reflect_int64   = int64(0)
	reflect_float32 = float32(0)
	reflect_float64 = float64(0)
	reflect_string  = ""
	// possible indicators for NULL, SET, ENUM, GEOMETRY?
	// reflect_empty   = struct{}{}
	// reflect_many    = []interface{}{}
)

var ( // reflect.Types
	// non-null types
	typeUint8   = reflect.TypeOf(reflect_uint8)
	typeUint16  = reflect.TypeOf(reflect_uint16)
	typeUint32  = reflect.TypeOf(reflect_uint32)
	typeUint64  = reflect.TypeOf(reflect_uint64)
	typeInt8    = reflect.TypeOf(reflect_int8)
	typeInt16   = reflect.TypeOf(reflect_int16)
	typeInt32   = reflect.TypeOf(reflect_int32)
	typeInt64   = reflect.TypeOf(reflect_int64)
	typeFloat32 = reflect.TypeOf(reflect_float32)
	typeFloat64 = reflect.TypeOf(reflect_float64)
	typeString  = reflect.TypeOf(reflect_string)
	typeBigint  = reflect.TypeOf(big.NewInt(0))
	typeBools   = reflect.TypeOf([]bool{})
	typeBytes   = reflect.TypeOf([]byte{})
	typeTime    = reflect.TypeOf(time.Time{})
	// nullable types
	typeNullInt64   = reflect.TypeOf(sql.NullInt64{})
	typeNullFloat64 = reflect.TypeOf(sql.NullFloat64{})
	typeNullString  = reflect.TypeOf(sql.NullString{})
	typeNullTime    = reflect.TypeOf(mysql.NullTime{})
	// typeNullBool doesn't match in MySQL, boolean is (unsigned?) tinyint(1),
	// it may have more than 2 states
	//typeNullBool = reflect.TypeOf(sql.NullBool{})
)

// retrieve the best matching reflect.Type for the mysql field.
// Returns an error if no matching type exists.
func (f mysqlField) ReflectGoType() (reflect.Type, error) {
	if f.IsUnsigned() {
		switch f.fieldType {
		case fieldTypeTiny:
			return typeUint8, nil
		case fieldTypeShort:
			return typeUint16, nil
		case fieldTypeInt24, fieldTypeLong:
			return typeUint32, nil
		case fieldTypeLongLong:
			return typeUint64, nil
		}
		// unsigned non-integer types fall through
	}
	switch f.fieldType {
	case fieldTypeTiny:
		return typeInt8, nil
	case fieldTypeShort:
		return typeInt16, nil
	case fieldTypeInt24, fieldTypeLong:
		return typeInt32, nil
	case fieldTypeLongLong:
		return typeInt64, nil
	case fieldTypeFloat:
		return typeFloat32, nil
	case fieldTypeDouble:
		return typeFloat64, nil
	case fieldTypeDecimal, fieldTypeNewDecimal:
		return typeBigint, nil
	case fieldTypeYear, fieldTypeDate, fieldTypeNewDate, fieldTypeTime, fieldTypeTimestamp, fieldTypeDateTime:
		return typeTime, nil
	case fieldTypeBit:
		return typeBools, nil
	case fieldTypeVarChar, fieldTypeVarString, fieldTypeString:
		return typeString, nil
	case fieldTypeTinyBLOB, fieldTypeMediumBLOB, fieldTypeBLOB, fieldTypeLongBLOB:
		return typeBytes, nil
	case fieldTypeEnum, fieldTypeSet, fieldTypeGeometry, fieldTypeNULL:
		return nil, errorTypeMismatch(f.fieldType)
	}
	return nil, errors.New("unknown mysql type")
}

// retrieve the best matching reflect.Type for the mysql field.
// Returns an error if no matching type exists.
func (f mysqlField) ReflectSqlType(forceNullable bool) (reflect.Type, error) {
	if forceNullable || !f.IsNotNull() {
		switch {
		case f.IsInteger():
			return typeNullInt64, nil
		case f.IsFloatingPoint():
			return typeNullFloat64, nil
		case f.IsText():
			return typeNullString, nil
		case f.IsTime():
			return typeNullTime, nil
		case f.IsBlob():
			return typeBytes, nil // []byte can be nil on its own
		}
		// All other types are not nullable in Go right now
		return nil, errorTypeMismatch(f.fieldType)
	}
	return f.ReflectGoType()
}

type errorTypeMismatch uint8

func (e errorTypeMismatch) Error() string {
	return "no matching go type for " + mysqlNameFor(uint8(e))
}

func mysqlNameFor(fieldType uint8) string {
	switch fieldType {
	// --- integer ---
	case fieldTypeTiny:
		return "TINYINT"
	case fieldTypeShort:
		return "SMALLINT"
	case fieldTypeInt24, fieldTypeLong:
		return "INT"
	case fieldTypeLongLong:
		return "BIGINT"
	// --- floating point ---
	case fieldTypeFloat:
		return "FLOAT"
	case fieldTypeDouble:
		return "DOUBLE"
	// --- decimal ---
	case fieldTypeDecimal, fieldTypeNewDecimal:
		return "DECIMAL"
	// --- date & time ---
	case fieldTypeYear:
		return "YEAR"
	case fieldTypeDate, fieldTypeNewDate:
		return "DATE"
	case fieldTypeTime:
		return "TIME"
	case fieldTypeTimestamp:
		return "TIMESTAMP"
	case fieldTypeDateTime:
		return "DATETIME"
	// --- null ---
	case fieldTypeNULL:
		return "NULL"
	// --- bit ---
	case fieldTypeBit:
		return "BIT"
	// --- string ---
	case fieldTypeVarChar, fieldTypeVarString:
		return "VARCHAR"
	case fieldTypeString:
		return "CHAR"
	// --- enum ---
	case fieldTypeEnum:
		return "ENUM"
	// --- set ---
	case fieldTypeSet:
		return "SET"
	// --- blob ---
	case fieldTypeTinyBLOB:
		return "TINY BLOB"
	case fieldTypeMediumBLOB:
		return "MEDIUM BLOB"
	case fieldTypeBLOB:
		return "BLOB"
	case fieldTypeLongBLOB:
		return "LONG BLOB"
	// --- geometry ---
	case fieldTypeGeometry:
		return "GEOMETRY"
	}
	return ""
}

type parameterType uint

const (
	// unknown type, no information about parameter requirements
	ParamUnknown parameterType = iota
	// requires no parameters in MySQL declaration
	ParamNone
	// requires no parameters or length (int > 0) in MySQL declaration
	ParamMayLength
	// requires length (int > 0) in MySQL declaration
	ParamMustLength
	// requires no parameters or length (int > 0) and decimals (int >= 0) in MySQL declaration
	// OBSOLETE since decimals are contained in mysqlField...
	// ParamMayLengthAndDecimals
	_
	// requires no parameters or length (int > 0) or length and decimals (int >= 0) in MySQL declaration
	// OBSOLETE since decimals are contained in mysqlField...
	// ParamMayLengthMayDecimals
	_
	// requires valid values as parameters in MySQL declaration
	ParamValues
)

// retrieve information about parameters used in MysqlDeclaration
func (f mysqlField) MysqlParameters() parameterType {
	switch f.fieldType {
	case // date types, *BLOB and GEOMETRY declarations have no parameters
		fieldTypeYear, fieldTypeDate, fieldTypeNewDate,
		fieldTypeTinyBLOB, fieldTypeMediumBLOB, fieldTypeBLOB, fieldTypeLongBLOB,
		fieldTypeGeometry,
		// time types use decimals: microseconds
		fieldTypeTime, fieldTypeTimestamp, fieldTypeDateTime:
		return ParamNone
	case // BIT, *INT* and CHAR declarations have one optional parameter (length)
		fieldTypeBit,
		fieldTypeTiny, fieldTypeShort, fieldTypeInt24, fieldTypeLong, fieldTypeLongLong,
		fieldTypeString,
		// DECIMAL and NUMERIC declarations have one optional parameter (length) and may use decimals
		fieldTypeDecimal, fieldTypeNewDecimal,
		// REAL, FLOAT and DOUBLE declarations have one optional parameter (length, will also use decimals when length is given)
		fieldTypeFloat, fieldTypeDouble:
		return ParamMayLength
	case // VARCHAR and VARBINARY declarations have one mandatory parameter (length)
		fieldTypeVarChar, fieldTypeVarString:
		return ParamMustLength
	case // ENUM and SET declarations have multiple parameters
		fieldTypeEnum, fieldTypeSet:
		return ParamValues
	}
	return ParamUnknown
}

type paramErr string

func (p paramErr) Error() string {
	return string(p)
}

// mysql type declaration
// The declaration includes the type and size and the attributes "NOT NULL", "ZEROFILL" and "BINARY".
// It does not include the name, character sets, collations, default value, keys or the attribute auto_increment.
// For BIT, all INT types, CHAR and BINARY types, args is optional and may be one int: length.
// For VARCHAR and VARBINARY types, args must be one int: length.
// For DECIMAL and NUMERIC types, it may be none or one int: length.
// For DATETIME, TIME, TIMESTAMP, decimals is used for microseconds.
// For FLOAT, DOUBLE and REAL floating point types, it is optional and, when given, must be two ints: length and decimals.
// For SETs and ENUMs, it specifies the possible values.
// For all other types, args must be empty.
func (f mysqlField) MysqlDeclaration(args ...interface{}) (string, error) {
	const (
		unsigned = " UNSIGNED"
		notNull  = " NOT NULL"
		zerofill = " ZEROFILL"
		binary   = " BINARY"
		// errors
		errNil        = paramErr("can't create declaration for NULL")
		errUnknown    = paramErr("parameter error, unknown")
		errNone       = paramErr("parameter error, must be none")
		errMayLength  = paramErr("parameter error, must be none or one int (length)")
		errMustLength = paramErr("parameter error, must be one int (length)")
		errEnumOrSet  = paramErr("parameter error, must be at least one entry")
	)
	// fail fast if we can't provide a declaration
	if f.fieldType == fieldTypeNULL {
		return "", errNil
	}
	var param, us, nn, zf, bin string
	if f.IsNotNull() {
		// any type may be "NOT NULL"
		nn = notNull
	}
	switch f.fieldType {
	case fieldTypeFloat, fieldTypeDouble,
		fieldTypeDecimal, fieldTypeNewDecimal:
		if len(args) == 1 {
			param = fmt.Sprintf("(%d,%d)", args[0], f.decimals)
		}
		fallthrough
	case // numeric types may be unsigned or zerofill
		fieldTypeTiny, fieldTypeShort, fieldTypeInt24, fieldTypeLong, fieldTypeLongLong:
		if f.IsUnsigned() {
			us = unsigned
		}
		if f.IsZerofill() {
			zf = zerofill
		}
	case fieldTypeBit:
		if len(args) != 1 {
			return "", errMustLength
		}
		param = fmt.Sprintf("(%d)", args[0])
	case fieldTypeYear, fieldTypeDate, fieldTypeNewDate,
		fieldTypeTinyBLOB, fieldTypeMediumBLOB, fieldTypeBLOB, fieldTypeLongBLOB,
		fieldTypeGeometry:
		// nothing to be done for these types
	case // only string types may be binary
		fieldTypeVarChar, fieldTypeVarString:
		if f.IsBinary() {
			bin = binary
		}
		if len(args) != 1 {
			return "", errMustLength
		}
		param = fmt.Sprintf("(%d)", args[0])
	case fieldTypeString:
		if f.IsBinary() {
			bin = binary
		}
		if len(args) == 1 {
			param = fmt.Sprintf("(%d)", args[0])
		}
	case fieldTypeTime, fieldTypeTimestamp, fieldTypeDateTime:
		if f.decimals > 0 {
			param = fmt.Sprintf("(%d)", f.decimals)
		}

	case fieldTypeEnum, fieldTypeSet:
		if len(args) == 0 {
			return "", errEnumOrSet
		}
		param = fmt.Sprintf("(%v)", args...)
	default:
		return "", errUnknown
	}
	return mysqlNameFor(f.fieldType) + param + bin + us + zf + nn, nil
}
