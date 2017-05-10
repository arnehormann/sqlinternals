// sqlinternals for github.com/go-sql-driver/mysql - retrieve column metadata from sql.*Row / sql.*Rows
//
// Copyright 2013 Arne Hormann. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysqlinternals

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"sync"
	"unsafe"

	"github.com/arnehormann/sqlinternals"
)

// keep in sync with github.com/go-sql-driver/mysql/const.go
const (
	fieldTypeDecimal byte = iota
	fieldTypeTiny
	fieldTypeShort
	fieldTypeLong
	fieldTypeFloat
	fieldTypeDouble
	fieldTypeNULL
	fieldTypeTimestamp
	fieldTypeLongLong
	fieldTypeInt24
	fieldTypeDate
	fieldTypeTime
	fieldTypeDateTime
	fieldTypeYear
	fieldTypeNewDate
	fieldTypeVarChar
	fieldTypeBit
)
const (
	fieldTypeJSON byte = iota + 0xf5
	fieldTypeNewDecimal
	fieldTypeEnum
	fieldTypeSet
	fieldTypeTinyBLOB
	fieldTypeMediumBLOB
	fieldTypeLongBLOB
	fieldTypeBLOB
	fieldTypeVarString
	fieldTypeString
	fieldTypeGeometry
)

type fieldFlag uint16

const (
	flagNotNULL fieldFlag = 1 << iota
	flagPriKey
	flagUniqueKey
	flagMultipleKey
	flagBLOB
	flagUnsigned
	flagZeroFill
	flagBinary
	flagEnum
	flagAutoIncrement
	flagTimestamp
	flagSet
	flagUnknown1
	flagUnknown2
	flagUnknown3
	flagUnknown4
)

// keep mysqlRows and mysqlField in sync with structs in github.com/go-sql-driver/rows.go
type mysqlField struct {
	tableName string
	name      string
	flags     fieldFlag
	fieldType byte
	decimals  byte
}

type resultSet struct {
	columns []mysqlField
	done    bool
}

type mysqlRows struct {
	mc *mysqlConn
	rs resultSet
}

type emptyRows struct{}

type rowEmbedder struct {
	mysqlRows
}

// dummy for mysqlRows
type mysqlConn struct{}

// internals
type mysqlError string

func (e mysqlError) Error() string {
	return string(e)
}

const (
	errUnexpectedNil  = mysqlError("wrong argument, rows must not be nil")
	errUnexpectedType = mysqlError("wrong argument, must be *mysql.mysqlRows")
	rowtypeBinary     = "binaryRows"
	rowtypeText       = "textRows"
	rowtypeEmpty      = "emptyRows"
)

var (
	// populate the offset only once
	initMutex      sync.Mutex
	failedInit     bool
	structsChecked bool
)

// canConvert returns true if the memory layout and the struct field names of
// 'from' match those of 'to'.
func canConvert(from, to reflect.Type) bool {
	switch {
	case from.Kind() != reflect.Struct,
		from.Kind() != to.Kind(),
		from.Size() != to.Size(),
		from.Name() != to.Name(),
		from.NumField() != to.NumField():
		return false
	}
	for i, max := 0, from.NumField(); i < max; i++ {
		sf, tf := from.Field(i), to.Field(i)
		if sf.Name != tf.Name || sf.Offset != tf.Offset {
			return false
		}
		tsf, ttf := sf.Type, tf.Type
		for done := false; !done; {
			k := tsf.Kind()
			if k != ttf.Kind() {
				return false
			}
			switch k {
			case reflect.Array, reflect.Chan, reflect.Map, reflect.Ptr, reflect.Slice:
				tsf, ttf = tsf.Elem(), ttf.Elem()
			case reflect.Interface:
				// don't have to handle matching interfaces here
				if tsf != ttf {
					// there are none in our case, so we are extra strict
					return false
				}
			case reflect.Struct:
				if tsf.Name() != ttf.Name() {
					return false
				}
				done = true
			default:
				done = true
			}
		}
	}
	return true
}

func initOffsets(rows driver.Rows) error {
	const (
		errWrapperMismatch   = mysqlError("unexpected structure of textRows or binaryRows")
		errRowsMismatch      = mysqlError("unexpected structure of mysqlRows")
		errResultsetMismatch = mysqlError("unexpected structure of resultSet")
		errFieldMismatch     = mysqlError("unexpected structure of mysqlField")
	)
	// make sure mysqlRows is the right type (full certainty is impossible).
	if rows == nil {
		return errUnexpectedNil
	}
	argType := reflect.TypeOf(rows)
	if argType.Kind() != reflect.Ptr {
		return errUnexpectedType
	}
	elemType := argType.Elem()
	if elemType.Kind() != reflect.Struct {
		return errUnexpectedType
	}
	switch typeName := elemType.Name(); typeName {
	case rowtypeBinary, rowtypeText:
	default:
		return errUnexpectedType
	}
	embedded, ok := elemType.FieldByName("mysqlRows")
	if !ok {
		return errWrapperMismatch
	}
	elemType = embedded.Type
	// compare mysqlRows
	if !canConvert(elemType, reflect.TypeOf(mysqlRows{})) {
		return errRowsMismatch
	}
	resultSetField, ok := elemType.FieldByName("rs")
	if !ok {
		return errRowsMismatch
	}
	elemType = resultSetField.Type
	// compare resultSet
	if !canConvert(elemType, reflect.TypeOf(resultSet{})) {
		return errRowsMismatch
	}
	colsField, ok := elemType.FieldByName("columns")
	if !ok {
		return errResultsetMismatch
	}
	// compare mysqlField
	if !canConvert(colsField.Type.Elem(), reflect.TypeOf(mysqlField{})) {
		fmt.Printf("=> %#v\n\n", reflect.Zero(colsField.Type.Elem()).Interface())
		return errFieldMismatch
	}
	return nil
}

func driverRows(rowOrRows interface{}) (driver.Rows, bool) {
	if rowOrRows == nil || failedInit {
		return nil, false
	}
	rows, err := sqlinternals.Inspect(rowOrRows)
	if err != nil || rows == nil {
		return nil, false
	}
	dRows, ok := rows.(driver.Rows)
	if !ok {
		return nil, false
	}
	if uninitialized := !structsChecked; uninitialized {
		ok = true
		initMutex.Lock()
		defer initMutex.Unlock()
		if !failedInit {
			switch err = initOffsets(dRows); err {
			case nil:
				structsChecked = true
				uninitialized = false
			case errUnexpectedType, errUnexpectedNil:
				ok = false
			default:
				failedInit = true
				ok = false
			}
			if !ok {
				return nil, false
			}
		}
	}
	return dRows, true
}

// IsBinary reports whether the row value was retrieved using the binary protocol.
//
// MySQL results retrieved with prepared statements or Query with additional arguments
// use the binary protocol. The results are typed, the driver will use the closest
// matching Go type.
// A plain Query call with only the query itself will not use the binary protocol but the
// text protocol. The results are all strings in that case.
func IsBinary(rowOrRows interface{}) (bool, error) {
	const errUnavailable = mysqlError("IsBinary is not available")
	dRows, ok := driverRows(rowOrRows)
	if !ok {
		return false, errUnavailable
	}
	argType := reflect.TypeOf(dRows)
	return rowtypeBinary == argType.Elem().Name(), nil
}

// Columns retrieves a []Column for sql.Rows or sql.Row with type inspection abilities.
//
// The field indices match those of a call to Columns().
// Returns an error if the argument is not sql.Rows or sql.Row based on github.com/go-sql-driver/mysql.
func Columns(rowOrRows interface{}) ([]Column, error) {
	const errUnavailable = mysqlError("Columns is not available")
	dRows, ok := driverRows(rowOrRows)
	if !ok {
		return nil, errUnavailable
	}
	if rowtypeEmpty == reflect.TypeOf(dRows).Name() {
		return nil, nil
	}
	cols := (*mysqlRows)((unsafe.Pointer)(reflect.ValueOf(dRows).Pointer())).rs.columns
	columns := make([]Column, len(cols))
	for i, c := range cols {
		columns[i] = c
	}
	return columns, nil
}
