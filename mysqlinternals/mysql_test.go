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
	_ "github.com/go-sql-driver/mysql"
	"os"
	"reflect"
	"testing"
)

var dsn string

type Scanner interface {
	Scan(values ...interface{}) error
}

func init() {
	if envdsn := os.Getenv("MYSQL_DSN"); envdsn != "" {
		dsn = envdsn
	} else {
		dsn = "root@tcp(127.0.0.1:3306)/"
	}
}

func testRow(t *testing.T, test typeTest, useQueryRow bool) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// check that it is accessible and matches the one in tester.rows
	var source Scanner
	var cols []Column
	switch {
	case useQueryRow:
		var row *sql.Row
		row, err = db.QueryRow(test.query, test.queryArgs...), nil
		if err != nil {
			break
		}
		source = row
	case !useQueryRow:
		var rows *sql.Rows
		rows, err = db.Query(test.query, test.queryArgs...)
		if err != nil {
			break
		}
		defer rows.Close()
		source = rows
	}
	if err != nil {
		t.Fatal(err)
	}
	cols, err = Columns(source)
	if err != nil {
		t.Fatal(err)
	}

	col := cols[0]
	decl, derr := col.MysqlDeclaration(test.sqlDeclParams...)
	if test.sqlTypeError && derr == nil {
		t.Errorf("SQL: expected an error in MysqlDeclaration\n")
	}
	if !test.sqlTypeError && derr != nil {
		t.Errorf("SQL: did not expect an error in MysqlDeclaration, got '%v'\n", derr)
	}
	if decl != test.sqlType {
		t.Errorf("SQL: type '%s' did not match expected '%s'\n", decl, test.sqlType)
	}
	refl, rerr := col.ReflectType()
	if test.goTypeError && rerr == nil {
		t.Errorf("Go: expected an error in ReflectType\n")
	}
	if !test.goTypeError && rerr != nil {
		t.Errorf("Go: did not expect an error in ReflectType, got '%v'\n", rerr)
	}
	if refl != test.goType {
		t.Errorf("Go: type '%s' did not match expected '%s'\n", refl, test.goType)
	}
	if test.hasValue {
		if rows, ok := source.(*sql.Rows); ok && !rows.Next() {
			t.Error("could not scan from sql.Rows")
		}
		err = source.Scan(&test.receiver)
		if err != nil {
			t.Error(err)
			return
		}
		eVal := reflect.ValueOf(test.expectedValue)
		rVal := reflect.ValueOf(test.receiver)
		if eVal.Type() != rVal.Type() {
			t.Errorf("types of expected (%s) and received (%s) values didn't match\n",
				eVal.Type(), rVal.Type())
		}
		// TODO: compare value and assignability
	}
}

func args(v ...interface{}) []interface{} {
	return v
}

type typeTest struct {
	id            string
	query         string
	queryArgs     []interface{}
	sqlType       string
	sqlDeclParams []interface{}
	sqlTypeError  bool
	goType        reflect.Type
	goTypeError   bool
	hasValue      bool
	expectedValue interface{}
	receiver      interface{}
}

func TestRows(t *testing.T) {
	testSetups := []typeTest{
		typeTest{
			id:            "select string (text mode)",
			query:         "select 'Hi'",
			sqlType:       "VARCHAR(2) NOT NULL",
			sqlDeclParams: args(2),
			sqlTypeError:  false,
			goType:        reflect.TypeOf(""),
			goTypeError:   false,
			hasValue:      true,
			expectedValue: []byte("Hi"),
		},
		typeTest{
			id:            "select string (binary mode)",
			query:         "select ?",
			queryArgs:     args("Hi"),
			sqlType:       "CHAR(2) NOT NULL",
			sqlDeclParams: args(2),
			sqlTypeError:  false,
			goType:        reflect.TypeOf(""),
			goTypeError:   false,
			hasValue:      true,
			expectedValue: []byte("Hi"),
		},
		// TODO: add more tests (many columns, NULL column, different types...)
	}
	for _, setup := range testSetups {
		testRow(t, setup, false)
	}
}
