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
	"github.com/arnehormann/mirror"
	"github.com/arnehormann/sqlinternals"
	"reflect"
	"sync"
	"unsafe"
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
	fieldTypeNewDecimal byte = iota + 0xf6
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
	fieldType byte
	flags     fieldFlag
	name      string
}

type mysqlRows struct {
	mc      *mysqlConn
	columns []mysqlField
}

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
)

var (
	// populate the offset only once
	initMutex      sync.Mutex
	failedInit     bool
	structsChecked bool
)

func initOffsets(rows driver.Rows) error {
	const (
		errWrapperMismatch = mysqlError("unexpected structure of textRows or binaryRows")
		errRowsMismatch    = mysqlError("unexpected structure of mysqlRows")
		errFieldMismatch   = mysqlError("unexpected structure of mysqlField")
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
	case "textRows", "binaryRows":
	default:
		return errUnexpectedType
	}
	if elemType.NumField() != 1 {
		return errWrapperMismatch
	}
	embedded := elemType.Field(0)
	if embedded.Name != "mysqlRows" {
		return errWrapperMismatch
	}
	elemType = embedded.Type
	// compare mysqlRows
	if !mirror.CanConvert(elemType, reflect.TypeOf(mysqlRows{})) {
		return errRowsMismatch
	}
	colsField, ok := elemType.FieldByName("columns")
	if !ok {
		return errRowsMismatch
	}
	// compare mysqlField
	if !mirror.CanConvert(colsField.Type.Elem(), reflect.TypeOf(mysqlField{})) {
		return errFieldMismatch
	}
	return nil
}

// Columns retrieves a []Column for sql.Rows or sql.Row with type inspection abilities.
// The field indices match those of a call to Columns().
// Returns an error if the argument is not sql.Rows or sql.Row based on github.com/go-sql-driver/mysql.
func Columns(rowOrRows interface{}) ([]Column, error) {
	const errUnavailable = mysqlError("Columns is not available")
	if rowOrRows == nil {
		return nil, errUnavailable
	}
	rows, err := sqlinternals.Inspect(rowOrRows)
	if err != nil || rows == nil {
		return nil, errUnavailable
	}
	dRows, ok := rows.(driver.Rows)
	if !ok {
		return nil, errUnavailable
	}
	if uninitialized := !structsChecked; uninitialized {
		// The logic in this can fail, but it is faster for the common case.
		// The failure would be caused when more than one goroutine access
		// Columns for the very first time and initOffset fails because
		// the first goroutine entering the mutex passed a struct belonging to another driver.
		// But even then, it will recover on subsequent calls.
		initMutex.Lock()
		defer initMutex.Unlock()
		if !failedInit {
			switch err = initOffsets(dRows); err {
			case nil:
				structsChecked = true
				uninitialized = false
			case errUnexpectedType, errUnexpectedNil:
				return nil, errUnavailable
			default:
				failedInit = true
				return nil, err
			}
		}
	}
	cols := (*mysqlRows)((unsafe.Pointer)(reflect.ValueOf(dRows).Pointer())).columns
	columns := make([]Column, len(cols))
	for i, c := range cols {
		columns[i] = c
	}
	return columns, nil
}
