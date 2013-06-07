// sqlinternals - retrieve driver.Rows from sql.*Row / sql.*Rows
//
// Copyright 2013 Arne Hormann. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlinternals

import (
	"database/sql"
	"database/sql/driver"
	"io"
	"testing"
)

type omnithing struct {
	numInputs int
	columns   []string
	rows      [][]interface{}
}

func (t *omnithing) Close() error { return nil }

// driver.Driver
func (t *omnithing) Open(name string) (driver.Conn, error) { return t, nil }

// driver.Conn
func (t *omnithing) Prepare(query string) (driver.Stmt, error) { return t, nil }
func (t *omnithing) Begin() (driver.Tx, error)                 { return t, nil }

// driver.Tx
func (t *omnithing) Commit() error   { return nil }
func (t *omnithing) Rollback() error { return nil }

// driver.Stmt
func (t *omnithing) NumInput() int                                   { return t.numInputs }
func (t *omnithing) Exec(args []driver.Value) (driver.Result, error) { return t, nil }
func (t *omnithing) Query(args []driver.Value) (driver.Rows, error)  { return t, nil }

// driver.Result
func (t *omnithing) LastInsertId() (int64, error) { return 0, nil }
func (t *omnithing) RowsAffected() (int64, error) { return 0, nil }

// driver.Rows
func (t *omnithing) Columns() []string { return t.columns }
func (t *omnithing) Next(dest []driver.Value) error {
	if len(t.rows) == 0 {
		return io.EOF
	}
	var row []interface{}
	row, t.rows = t.rows[0], t.rows[1:]
	for i, v := range row {
		dest[i] = v
	}
	return nil
}

func (o *omnithing) setDB(numInputs int, columns []string, cells ...interface{}) *omnithing {
	o.numInputs = numInputs
	o.columns = columns
	numCols, numCells := len(columns), len(cells)
	numRows := numCells / numCols
	if numCols*numRows != numCells {
		panic("wrong number of cells")
	}
	rows := [][]interface{}{}
	for r := 0; r < numRows; r++ {
		cols := []interface{}{}
		for c := 0; c < numCols; c++ {
			cols = append(cols, cells[r*numCols+c])
		}
		rows = append(rows, cols)
	}
	o.rows = rows
	return o
}

type querier func(conn *sql.DB) (interface{}, error)

var (
	testdriver = &omnithing{}
	// make sure the test type implements the interfaces
	_ driver.Driver = testdriver
	_ driver.Conn   = testdriver
	_ driver.Tx     = testdriver
	_ driver.Stmt   = testdriver
	_ driver.Result = testdriver
	_ driver.Rows   = testdriver
)

const driverType = "test"

func init() {
	sql.Register(driverType, testdriver)
}

func runRowsTest(t *testing.T, query querier, numInputs int, columns []string, cells ...interface{}) {
	// set intial state before usage
	testdriver.setDB(numInputs, columns, cells...)
	// run a query, retrieve *sql.Rows
	conn, err := sql.Open(driverType, "")
	defer conn.Close()
	rowOrRows, err := query(conn)
	if closer, ok := rowOrRows.(io.Closer); ok {
		defer closer.Close()
	}
	// check that it is accessible and matches the one in testdriver.rows
	unwrapped, err := Inspect(rowOrRows)
	if err != nil {
		t.Error(err)
		return
	}
	myrows, ok := unwrapped.(*omnithing)
	if !ok || myrows != testdriver {
		t.Errorf("returned driver.Rows must match those passed in.")
	}
}

func TestRowWithoutArgs(t *testing.T) {
	query := func(conn *sql.DB) (interface{}, error) {
		return conn.QueryRow(`SELECT "test"`), nil
	}
	runRowsTest(t, query, 0, []string{"header"}, "test")
}

func TestRowWithArgs(t *testing.T) {
	query := func(conn *sql.DB) (interface{}, error) {
		return conn.QueryRow(`SELECT ?`, "test"), nil
	}
	runRowsTest(t, query, 1, []string{"header"}, "test")
}

func TestRowsWithoutArgs(t *testing.T) {
	query := func(conn *sql.DB) (interface{}, error) {
		return conn.Query(`SELECT "test"`)
	}
	runRowsTest(t, query, 0, []string{"header"}, "test")
}

func TestRowsWithArgs(t *testing.T) {
	query := func(conn *sql.DB) (interface{}, error) {
		return conn.Query(`SELECT ?`, "test")
	}
	runRowsTest(t, query, 1, []string{"header"}, "test")
}
