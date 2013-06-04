package mysqlinternals

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"io"
	"os"
	"testing"
)

var dsn string

func init() {
	if envdsn := os.Getenv("MYSQL_DSN"); envdsn != "" {
		dsn = envdsn
	} else {
		dsn = "root@tcp(127.0.0.1:3306)/"
	}
}

func getColumns(t *testing.T, querier func(db *sql.DB) (interface{}, error)) []Column {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rowOrRows, err := querier(db)
	if err != nil {
		t.Fatal(err)
	}
	if closer, ok := rowOrRows.(io.Closer); ok {
		defer closer.Close()
	}
	// check that it is accessible and matches the one in tester.rows
	columns, err := Columns(rowOrRows)
	if err != nil {
		t.Fatal(err)
	}
	return columns
}

func testRow(t *testing.Test, test typeTest, querySingleRow bool) {
	var cols []Column
	if querySingleRow {
		cols = getColumns(t, func(db *sql.DB) (interface{}, error) {
			return db.QueryRows(test.query, test.queryArgs...), nil
		})
	} else {
		cols = getColumns(t, func(db *sql.DB) (interface{}, error) {
			return db.Query(test.query, test.queryArgs...)
		})
	}
	decl, derr := col.MysqlDeclaration(test.sqlDeclParams...)
	refl, rerr := col.ReflectType()

}

type typeTest struct {
	id            string
	query         string
	queryArgs     []interface{}
	compareVals   func(a, b interface{}) bool
	sqlDeclParams []interface{}
	goVal         interface{}
	goTypeError   bool
	sqlTypeError  bool
}

func TestRows(t *testing.T) {
	expected := []expect{}
	cols := runRowsTest(t, func(db *sql.DB) (interface{}, error) {
		return db.Query("SELECT CAST('0000-00-00' as DATE)")
	})
	if len(cols) != 1 {
		t.Fatal("wrong col length")
	}
	// for experimentation, not done yet
	col := cols[0]
	decl, derr := col.MysqlDeclaration()
	refl, rerr := col.ReflectType()
	t.Errorf("[%v] GO(%s) <-> SQL(%s) [%v]", rerr, refl, decl, derr)
}
