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
	rows      *testRows
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
func (t *omnithing) Query(args []driver.Value) (driver.Rows, error)  { return t.rows, nil }

// driver.Result
func (t *omnithing) LastInsertId() (int64, error) { return 0, nil }
func (t *omnithing) RowsAffected() (int64, error) { return 0, nil }

type testRows struct {
	text string
}

// driver.Rows
func (t *testRows) Close() error {
	return nil
}

func (t *testRows) Columns() []string {
	return []string{"testcol"}
}

func (t *testRows) Next(dest []driver.Value) error {
	if t.text == "" {
		return io.EOF
	}
	dest[0] = t.text
	return nil
}

var (
	tester = &omnithing{}
	// make sure the test types implement the interfaces
	_ driver.Driver = tester
	_ driver.Conn   = tester
	_ driver.Tx     = tester
	_ driver.Stmt   = tester
	_ driver.Result = tester
	_ driver.Rows   = &testRows{}
)

const driverType = "test"

func init() {
	sql.Register(driverType, tester)
}

// set to the new values, return the old ones (enables double-defer trickery for reset after use)
func (t *omnithing) setState(inputs int, rows *testRows) (int, *testRows) {
	oldInputs, oldRows := t.numInputs, t.rows
	t.numInputs, t.rows = inputs, rows
	return oldInputs, oldRows
}

func runRowsTest(t *testing.T, inputs int, querier func(conn *sql.DB) (interface{}, error)) {
	// set intial state and restore it after usage
	defer tester.setState(tester.setState(inputs, &testRows{text: "data"}))
	// run a query, retrieve *sql.Rows
	conn, err := sql.Open(driverType, "")
	defer conn.Close()
	rowOrRows, err := querier(conn)
	if closer, ok := rowOrRows.(io.Closer); ok {
		defer closer.Close()
	}
	// check that it is accessible and matches the one in tester.rows
	unwrapped, err := Inspect(rowOrRows)
	if err != nil {
		t.Error(err)
	} else if myrows, ok := unwrapped.(*testRows); !ok || myrows != tester.rows {
		t.Errorf("returned driver.Rows must match those passed in.")
	}
}

func TestRowWithoutArgs(t *testing.T) {
	runRowsTest(t, 0, func(conn *sql.DB) (interface{}, error) {
		return conn.QueryRow("SELECT 1"), nil
	})
}

func TestRowWithArgs(t *testing.T) {
	runRowsTest(t, 1, func(conn *sql.DB) (interface{}, error) {
		return conn.QueryRow("SELECT ?", 1), nil
	})
}

func TestRowsWithoutArgs(t *testing.T) {
	runRowsTest(t, 0, func(conn *sql.DB) (interface{}, error) {
		return conn.Query("SELECT 1")
	})
}

func TestRowsWithArgs(t *testing.T) {
	runRowsTest(t, 1, func(conn *sql.DB) (interface{}, error) {
		return conn.Query("SELECT ?", 1)
	})
}
