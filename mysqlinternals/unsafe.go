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
	"github.com/arnehormann/sqlinternals"
	"github.com/arnehormann/sqlinternals/mirror"
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
type mysqlRows struct {
	mc      *mysqlConn
	binary  bool
	columns []mysqlField
	eof     bool
}

type mysqlField struct {
	name      string
	fieldType byte
	flags     fieldFlag
}

// dummy for mysqlRows
type mysqlConn struct{}

// internals
type mysqlError string

func (e mysqlError) Error() string {
	return string(e)
}

const (
	errUnexpectedType        = mysqlError("wrong argument, must be *mysql.mysqlRows")
	errUnexpectedStruct      = mysqlError("could not access cols []mysql.mysqlField")
	errUnexpectedFieldStruct = mysqlError("could not access field in mysql.mysqlField")
	errUnavailable           = mysqlError("Fields is not available")
)

var (
	// populate the offset only once
	initMutex      sync.Mutex
	failedInit     bool
	structsChecked bool
)

func initOffsets(rows driver.Rows) (bool, error) {
	// make sure mysqlRows is the right type (full certainty is impossible).
	if rows == nil {
		return false, nil
	}
	argType := reflect.TypeOf(rows)
	if argType.Kind() != reflect.Ptr {
		return false, errUnexpectedType
	}
	elemType := argType.Elem()
	if elemType.Kind() != reflect.Struct || elemType.Name() != "mysqlRows" {
		return false, errUnexpectedType
	}
	// compare mysqlRows
	if !mirror.CanConvertUnsafe(elemType, reflect.TypeOf(mysqlRows{}), 0) {
		return false, errUnexpectedStruct
	}
	colsField, ok := elemType.FieldByName("columns")
	if !ok {
		return false, errUnexpectedStruct
	}
	// compare mysqlField
	if !mirror.CanConvertUnsafe(colsField.Type.Elem(), reflect.TypeOf(mysqlField{}), 0) {
		return false, errUnexpectedStruct
	}
	return true, nil
}

// Columns retrieves a []Column for sql.Rows or sql.Row with type inspection abilities.
// The field indices match those of a call to Columns().
// Returns an error if the argument is not sql.Rows or sql.Row based on github.com/go-sql-driver/mysql.
func Columns(rowOrRows interface{}) ([]Column, error) {
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
		if !failedInit {
			initialized, err := initOffsets(dRows)
			switch {
			case initialized:
				structsChecked = true
				uninitialized = false
			case err != errUnexpectedType:
				failedInit = true
			}
		}
		initMutex.Unlock()
		if uninitialized {
			return nil, errUnavailable
		}
	}

	cols := (*mysqlRows)((unsafe.Pointer)(reflect.ValueOf(dRows).Pointer())).columns
	columns := make([]Column, len(cols))
	for i, c := range cols {
		columns[i] = c
	}
	return columns, nil
}
