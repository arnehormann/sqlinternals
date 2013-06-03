package sqlinternals

import (
	"database/sql"
	"database/sql/driver"
	"reflect"
	"unsafe"
)

var (
	// field offsets for unsafe access (types are checked beforehand)
	offsetRowRows   uintptr // database/sql/Row.rows: database/sql/*Rows
	offsetRowsRowsi uintptr // database/sql/Rows.rowsi: database/sql/driver/Rows
)

// internal error type
type internalErr string

func (e internalErr) Error() string {
	return string(e)
}

const (
	errArgNil       = internalErr("argument must not be nil")
	errArgWrongType = internalErr("argument was not *sql.Row or *sql.Rows")
	errRowRowsNil   = internalErr("'err' xor 'rows' in sql.Row must be nil")
	errRowsRowsiNil = internalErr("'rowsi driver.Rows' in sql.Rows is nil")
)

// a driver.Rows implementatiton so we are able
// to get a type assignable to driver.Rows with reflect
type dummyRows struct{}

func (d dummyRows) Columns() []string {
	return nil
}

func (d dummyRows) Close() error {
	return nil
}

func (d dummyRows) Next(dest []driver.Value) error {
	return nil
}

// basic type assertion, panic on error
func panicIfUnassignable(field reflect.StructField, assignable reflect.Type, panicMsg string) {
	fType := field.Type
	if assignable == fType || assignable.AssignableTo(fType) {
		return
	}
	panic(panicMsg + "; " + assignable.String() + " is not assignable to " + fType.String())
}

func init() {
	// all types we need to check as templates
	var (
		tRow        reflect.Type = reflect.TypeOf(sql.Row{})
		tRows       reflect.Type = reflect.TypeOf(sql.Rows{})
		tRowsPtr    reflect.Type = reflect.TypeOf(&sql.Rows{})
		tDriverRows reflect.Type = reflect.TypeOf((driver.Rows)(dummyRows{}))
	)
	var i, expectFields, fields int
	// sql.Row must have a field "rows sql/*Rows"
	for i, expectFields, fields = 0, 1, tRow.NumField(); i < fields; i++ {
		field := tRow.Field(i)
		switch field.Name {
		case "rows":
			panicIfUnassignable(field, tRowsPtr,
				"database/sql/Row.rows is not database/sql/*Rows")
			offsetRowRows = field.Offset
			expectFields--
		}
	}
	if expectFields != 0 {
		panic("unexpected structure of database/sql/Row")
	}
	// sql.Rows must have a field "rowsi driver/Rows"
	for i, expectFields, fields = 0, 1, tRows.NumField(); i < fields; i++ {
		if field := tRows.Field(i); field.Name == "rowsi" {
			panicIfUnassignable(field, tDriverRows,
				"database/sql/Rows.rowsi is not database/sql/driver/Rows")
			offsetRowsRowsi = field.Offset
			expectFields--
		}
	}
	if expectFields != 0 {
		panic("unexpected structure of database/sql/Rows")
	}
}

// Inspect extracts the internal driver.Rows from sql.Row or sql.Rows.
// This can be used by a driver to work around issue 5606 in legacy versions.
func Inspect(sqlStruct interface{}) (interface{}, error) {
	// All of this has to use unsafe to access unexported fields, but it's robust:
	// we checked the types and structure in init.
	if sqlStruct == nil {
		return nil, errArgNil
	}
	var rows *sql.Rows
	switch v := sqlStruct.(type) {
	case *sql.Row:
		// extract rows from sql/*Row, if v.rows is nil, an error is returned.
		rowsPtr := (uintptr)((unsafe.Pointer)(v)) + offsetRowRows
		unsafeRows := *(**sql.Rows)((unsafe.Pointer)(rowsPtr))
		if unsafeRows == nil {
			return nil, errRowRowsNil
		}
		rows = unsafeRows
	case *sql.Rows:
		rows = v
	default:
		return errArgWrongType, nil
	}
	// return rowsi from sql/*Rows, if rows.rowsi is nil an error is returned.
	rowsiPtr := offsetRowsRowsi + (uintptr)((unsafe.Pointer)(rows))
	rowsi := *(*driver.Rows)((unsafe.Pointer)(rowsiPtr))
	if rowsi == nil {
		return nil, errRowsRowsiNil
	}
	return rowsi, nil
}
