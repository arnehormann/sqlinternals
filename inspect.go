package sqlinternals

import (
	"database/sql"
	"database/sql/driver"
	"reflect"
)

var (
	// field indices for faster reflect access. Types are also checked
	rowErrIdx    int // database/sql/Row.err: error
	rowRowsIdx   int // database/sql/Row.rows: database/sql/*Rows
	rowsRowsiIdx int // database/sql/Rows.rowsi: database/sql/driver/Rows
)

// internal error type
// Used instead of import "errors" for two reasons:
// - is used nowhere else, making it a good template for an AssignableTo assertion
// - can be used in const
type internalErr string

func (e internalErr) Error() string {
	return string(e)
}

const (
	errArgNil       = internalErr("argument must not be nil")
	errArgWrongType = internalErr("argument was not *sql.Row or *sql.Rows")
	errRowRows      = internalErr("'rows *sql.Rows' in sql.Row could not be read")
	errRowErr       = internalErr("'err error' in sql.Row could not be read")
	errRowErrNil    = internalErr("'err error' in sql.Row is nil")
	errRowsRowsi    = internalErr("'rowsi driver.Rows' in sql.Rows could not be read")
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
		tErr        reflect.Type = reflect.TypeOf(errArgNil)
		tDriverRows reflect.Type = reflect.TypeOf((driver.Rows)(dummyRows{}))
	)
	var i, expectFields, fields int
	// sql.Row must have fields "rows sql/*Rows" and "err error"
	for i, expectFields, fields = 0, 2, tRow.NumField(); i < fields; i++ {
		field := tRow.Field(i)
		switch field.Name {
		case "err":
			panicIfUnassignable(field, tErr,
				"database/sql/Row.err is not error")
			rowErrIdx = i
			expectFields--
		case "rows":
			panicIfUnassignable(field, tRowsPtr,
				"database/sql/Row.rows is not database/sql/*Rows")
			rowRowsIdx = i
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
			rowsRowsiIdx = i
			expectFields--
		}
	}
	if expectFields != 0 {
		panic("unexpected structure of database/sql/Rows")
	}
}

// return rows and err from from sql/*Row;
// according to documentation, exactly one of the two is non-nil.
// If rows is non nil, it is returned and err is ignored.
// If both are nil, an internal error is returned.
func sqlRowsFromSqlRow(row *sql.Row) (*sql.Rows, error) {
	if row == nil {
		return nil, errArgNil
	}
	derefRow := reflect.ValueOf(row).Elem()
	innerRows := derefRow.Field(rowRowsIdx)
	if innerRows.CanInterface() && !innerRows.IsNil() {
		if rows, ok := innerRows.Interface().(*sql.Rows); ok {
			return rows, nil
		}
		return nil, errRowRows
	}
	rowErr := derefRow.Field(rowErrIdx)
	if !rowErr.CanInterface() {
		return nil, errRowErr
	}
	if err, ok := rowErr.Interface().(error); ok && err != nil {
		// return error from sql.Row.err
		return nil, err
	}
	return nil, errRowErrNil
}

// return rowsi from sql/*Rows;
// return an error if the argument or rowsi is nil or can't be read.
func driverRowsFromSqlRows(rows *sql.Rows) (driver.Rows, error) {
	if rows == nil {
		return nil, errArgNil
	}
	driverRows := reflect.ValueOf(*rows).Field(rowsRowsiIdx)
	if !driverRows.CanInterface() {
		return nil, errRowsRowsi
	}
	if result, ok := driverRows.Interface().(driver.Rows); ok && result != nil {
		return result, nil
	}
	return nil, errRowsRowsiNil
}

// Inspect uses reflect to extract a driver.Driver from sql.Row or sql.Rows.
// This can be used by a driver to work around issue 5606 in legacy versions.
func Inspect(sqlStruct interface{}) (interface{}, error) {
	if sqlStruct == nil {
		return nil, errArgNil
	}
	var rows *sql.Rows
	switch v := sqlStruct.(type) {
	case *sql.Row:
		var err error
		rows, err = sqlRowsFromSqlRow(v)
		if err != nil {
			return nil, err
		}
	case *sql.Rows:
		rows = v
	default:
		return errArgWrongType, nil
	}
	return driverRowsFromSqlRows(rows)
}
