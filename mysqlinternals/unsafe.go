package mysqlinternals

import (
	"database/sql/driver"
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

// check possibility to convert a struct to another with unsafe.Pointer
func canConvertUnsafe(from, to reflect.Type, recurseStructs int) bool {
	//fmt.Printf("CHECK:\n -\t%s\n -\t%s\n", from, to)
	if from.Kind() != reflect.Struct || from.Kind() != to.Kind() ||
		from.Name() != to.Name() || from.NumField() != to.NumField() {
		return false
	}
	for i, max := 0, from.NumField(); i < max; i++ {
		sf, tf := from.Field(i), to.Field(i)
		//fmt.Printf("checking [%d]:\n\t%s: %v\n\t%s: %v\n", i, sf.Name, sf.Type, tf.Name, tf.Type)
		if sf.Name != tf.Name || sf.Offset != tf.Offset {
			return false
		}
		tsf, ttf := sf.Type, tf.Type
		for done := false; !done; {
			k := tsf.Kind()
			if k != ttf.Kind() {
				return false
			}
			//fmt.Printf("\t->\t%s %s == %s %s\n", tsf, tsf.Kind(), ttf, ttf.Kind())
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
				if recurseStructs <= 0 && tsf.Name() != ttf.Name() {
					return false
				}
				done = true
			default:
				done = true
			}
		}
		if recurseStructs > 0 && !canConvertUnsafe(tsf, ttf, recurseStructs-1) {
			return false
		}
	}
	//fmt.Printf("CHECK - are castable\n")
	return true
}

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
	if !canConvertUnsafe(elemType, reflect.TypeOf(mysqlRows{}), 0) {
		return false, errUnexpectedStruct
	}
	colsField, ok := elemType.FieldByName("columns")
	if !ok {
		return false, errUnexpectedStruct
	}
	// compare mysqlField
	if !canConvertUnsafe(colsField.Type.Elem(), reflect.TypeOf(mysqlField{}), 0) {
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
