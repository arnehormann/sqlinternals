package mysqlinternals

import (
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"time"
)

type Column interface {

	// mysql.name

	Name() string

	// derived from mysqlField.fieldType

	MysqlType() string
	IsNumber() bool
	IsInteger() bool
	IsFloatingPoint() bool
	IsDecimal() bool
	IsText() bool
	IsBlob() bool
	IsTime() bool

	// derived from mysqlField.flags

	IsPrimaryKey() bool
	IsUniqueKey() bool
	IsMultipleKey() bool
	IsNotNull() bool
	IsUnsigned() bool
	IsZerofill() bool
	IsBinary() bool
	IsAutoIncrement() bool

	// derived from mysqlField.fieldType and mysqlField.flags
	MysqlParameters() parameterType
	MysqlDeclaration(params ...interface{}) (string, error)
	ReflectType() (reflect.Type, error)
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
)

var ( // reflect.Types
	reflect_bigint = big.NewInt(0)
	reflect_bools  = []bool{}
	reflect_bytes  = []byte{}
	reflect_time   = time.Time{}
	// possible indicators for NULL, SET, ENUM, GEOMETRY?
	// reflect_empty   = struct{}{}
	// reflect_many    = []interface{}{}

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
	typeBigint  = reflect.TypeOf(reflect_bigint)
	typeBools   = reflect.TypeOf(reflect_bools)
	typeBytes   = reflect.TypeOf(reflect_bytes)
	typeString  = reflect.TypeOf(reflect_string)
	typeTime    = reflect.TypeOf(reflect_time)
)

// retrieve the best matching reflect.Type for the mysql field.
// Returns an error if no matching type exists.
func (f mysqlField) ReflectType() (reflect.Type, error) {
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
		return "SHORTINT"
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
	ParamMayLengthAndDecimals
	// requires no parameters or length (int > 0) or length and decimals (int >= 0) in MySQL declaration
	ParamMayLengthMayDecimals
	// requires one or more parameters in MySQL declaration
	ParamOneOrMore
)

// retrieve information about parameters used in MysqlDeclaration
func (f mysqlField) MysqlParameters() parameterType {
	switch f.fieldType {
	case // temporal, *BLOB and GEOMETRY declarations have no parameters
		fieldTypeYear, fieldTypeDate, fieldTypeNewDate, fieldTypeTime, fieldTypeTimestamp, fieldTypeDateTime,
		fieldTypeTinyBLOB, fieldTypeMediumBLOB, fieldTypeBLOB, fieldTypeLongBLOB,
		fieldTypeGeometry:
		return ParamNone
	case // BIT, *INT* and CHAR declarations have one optional parameters (length)
		fieldTypeBit,
		fieldTypeTiny, fieldTypeShort, fieldTypeInt24, fieldTypeLong, fieldTypeLongLong,
		fieldTypeString:
		return ParamMayLength
	case // DECIMAL and NUMERIC declarations have no, one or two parameters (length, decimals)
		fieldTypeDecimal, fieldTypeNewDecimal:
		return ParamMayLengthMayDecimals
	case // REAL, FLOAT and DOUBLE declarations have no or two parameters (length, decimals)
		fieldTypeFloat, fieldTypeDouble:
		return ParamMayLengthAndDecimals
	case // VARCHAR and VARBINARY declarations have one mandatory parameter (length)
		fieldTypeVarChar, fieldTypeVarString:
		return ParamMustLength
	case // ENUM and SET declarations have multiple parameters
		fieldTypeEnum, fieldTypeSet:
		return ParamOneOrMore
	}
	return ParamUnknown
}

type paramErr string

func (p paramErr) Error() string {
	return string(p)
}

// mysql type declaration
// The declaration includes the type and size and the attributes "NOT NULL", "ZEROFILL" and "BINARY".
// It does not include name, character sets, collations, default value, keys or auto_increment.
// For BIT, *INT*, CHAR and BINARY types, args is optional and may be one int: length
// For VARCHAR and VARBINARY types, args must be one int: length.
// For DECIMAL and NUMERIC types, it should be up two ints: length and decimals (precision and scale in MySQL docs).
// For FLOAT, DOUBLE and REAL floating point types, it is optional and, when given, must be two ints: length and decimals.
// For SETs and ENUMs, it specifies the possible values
// For all other types, args must be empty.
func (f mysqlField) MysqlDeclaration(args ...interface{}) (string, error) {
	const (
		unsigned = " UNSIGNED"
		notNull  = " NOT NULL"
		zerofill = " ZEROFILL"
		binary   = " BINARY"
		// errors
		errNil                  = paramErr("can't create declaration for NULL")
		errUnknown              = paramErr("parameter error, unknown")
		errNone                 = paramErr("parameter error, must be none")
		errMayLength            = paramErr("parameter error, must be none or one int (length)")
		errMustLength           = paramErr("parameter error, must be one int (length)")
		errMayLengthAndDecimals = paramErr("parameter error, must be none or two ints (length, decimals)")
		errMayLengthMayDecimals = paramErr("parameter error, must be none, one int (length) or two ints (length, decimals)")
		errEnumOrSet            = paramErr("parameter error, must be at least one entry")
	)
	// fail fast if we can't provide a declaration
	if f.fieldType == fieldTypeNULL {
		return "", errNil
	}
	// this function converts arguments to a parameter list fit for a mysql type declaration
	// distinction is by error thrown
	// see http://dev.mysql.com/doc/refman/5.6/en/create-table.html for create table specification
	argsToParam := func(ptype parameterType, err error) (string, error) {
		if ptype == ParamUnknown {
			return "", errUnknown
		}
		argLen := len(args)
		if argLen == 0 { // we don't have any arguments
			switch ptype { // valid for these cases
			case ParamNone, ParamMayLength, ParamMayLengthAndDecimals, ParamMayLengthMayDecimals:
				return "", nil
			}
			return "", err // error otherwise
		}
		if ptype == ParamOneOrMore { // at least one argument, ok for set and enum
			return fmt.Sprintf("(%v)", args...), nil // TODO: does this cover all cases?
		}
		if argLen > 2 { // if it's not enum or set it must not have more than two arguments
			return "", err
		}
		var length, decimals int
		var ok bool
		if length, ok = args[0].(int); !ok || length <= 0 { // parse length (first arg)
			return "", err // error: length must be an int > 0
		}
		if argLen == 2 { // we have two args
			if decimals, ok = args[1].(int); !ok || decimals < 0 {
				return "", err // error: decimals must be an int >= 0
			}
			switch ptype {
			case ParamMayLengthAndDecimals, ParamMayLengthMayDecimals:
				// valid result for types with (length, decimals)
				return fmt.Sprintf("(%d,%d)", length, decimals), nil
			}
			return "", err // error otherwise
		}
		switch ptype {
		case ParamMayLength, ParamMustLength, ParamMayLengthMayDecimals:
			// valid result for types with (length)
			return fmt.Sprintf("(%d)", length), nil
		}
		return "", err // error otherwise
	}
	var param, us, nn, zf, bin string
	switch f.fieldType {
	case // numeric types may be unsigned or zerofill
		fieldTypeTiny, fieldTypeShort, fieldTypeInt24, fieldTypeLong, fieldTypeLongLong,
		fieldTypeFloat, fieldTypeDouble,
		fieldTypeDecimal, fieldTypeNewDecimal:
		if f.IsUnsigned() {
			us = unsigned
		}
		if f.IsZerofill() {
			zf = zerofill
		}
	case // only string types may be binary
		fieldTypeVarChar, fieldTypeVarString, fieldTypeString:
		if f.IsBinary() {
			bin = binary
		}
	}
	if f.IsNotNull() {
		// any type may be "NOT NULL"
		nn = notNull
	}
	var err error
	switch ptype := f.MysqlParameters(); ptype {
	case ParamNone:
		param, err = argsToParam(ptype, errNone)
	case ParamMayLength:
		param, err = argsToParam(ptype, errMayLength)
	case ParamMayLengthMayDecimals:
		param, err = argsToParam(ptype, errMayLengthMayDecimals)
	case ParamMayLengthAndDecimals:
		param, err = argsToParam(ptype, errMayLengthAndDecimals)
	case ParamMustLength:
		param, err = argsToParam(ptype, errMustLength)
	case ParamOneOrMore:
		param, err = argsToParam(ptype, errEnumOrSet)
	case ParamUnknown:
		return "", errUnknown
	}
	if err != nil {
		return "", err
	}
	return mysqlNameFor(f.fieldType) + param + bin + us + zf + nn, nil
}