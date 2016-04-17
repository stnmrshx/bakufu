/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package sqlutils

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stnmrshx/golib/log"
	"strconv"
	"strings"
	"sync"
)

type RowMap map[string]CellData

type CellData sql.NullString

func (this *CellData) MarshalJSON() ([]byte, error) {
	if this.Valid {
		return json.Marshal(this.String)
	} else {
		return json.Marshal(nil)
	}
}

func (this *CellData) NullString() *sql.NullString {
	return (*sql.NullString)(this)
}

type RowData []CellData

func (this *RowData) MarshalJSON() ([]byte, error) {
	cells := make([](*CellData), len(*this), len(*this))
	for i, val := range *this {
		d := CellData(val)
		cells[i] = &d
	}
	return json.Marshal(cells)
}

type ResultData []RowData

var EmptyResultData = ResultData{}

func (this *RowMap) GetString(key string) string {
	return (*this)[key].String
}

func (this *RowMap) GetStringD(key string, def string) string {
	if cell, ok := (*this)[key]; ok {
		return cell.String
	}
	return def
}

func (this *RowMap) GetInt64(key string) int64 {
	res, _ := strconv.ParseInt(this.GetString(key), 10, 0)
	return res
}

func (this *RowMap) GetNullInt64(key string) sql.NullInt64 {
	i, err := strconv.ParseInt(this.GetString(key), 10, 0)
	if err == nil {
		return sql.NullInt64{Int64: i, Valid: true}
	} else {
		return sql.NullInt64{Valid: false}
	}
}

func (this *RowMap) GetInt(key string) int {
	res, _ := strconv.Atoi(this.GetString(key))
	return res
}

func (this *RowMap) GetIntD(key string, def int) int {
	res, err := strconv.Atoi(this.GetString(key))
	if err != nil {
		return def
	}
	return res
}

func (this *RowMap) GetUint(key string) uint {
	res, _ := strconv.Atoi(this.GetString(key))
	return uint(res)
}

func (this *RowMap) GetUintD(key string, def uint) uint {
	res, err := strconv.Atoi(this.GetString(key))
	if err != nil {
		return def
	}
	return uint(res)
}

func (this *RowMap) GetBool(key string) bool {
	return this.GetInt(key) != 0
}

var knownDBs map[string]*sql.DB = make(map[string]*sql.DB)
var knownDBsMutex = &sync.Mutex{}

func GetDB(mysql_uri string) (*sql.DB, bool, error) {
	knownDBsMutex.Lock()
	defer func() {
		knownDBsMutex.Unlock()
	}()

	var exists bool
	if _, exists = knownDBs[mysql_uri]; !exists {
		if db, err := sql.Open("mysql", mysql_uri); err == nil {
			knownDBs[mysql_uri] = db
		} else {
			return db, exists, err
		}
	}
	return knownDBs[mysql_uri], exists, nil
}

func RowToArray(rows *sql.Rows, columns []string) []CellData {
	buff := make([]interface{}, len(columns))
	data := make([]CellData, len(columns))
	for i, _ := range buff {
		buff[i] = data[i].NullString()
	}
	rows.Scan(buff...)
	return data
}

func ScanRowsToArrays(rows *sql.Rows, on_row func([]CellData) error) error {
	columns, _ := rows.Columns()
	for rows.Next() {
		arr := RowToArray(rows, columns)
		err := on_row(arr)
		if err != nil {
			return err
		}
	}
	return nil
}

func rowToMap(row []CellData, columns []string) map[string]CellData {
	m := make(map[string]CellData)
	for k, data_col := range row {
		m[columns[k]] = data_col
	}
	return m
}

func ScanRowsToMaps(rows *sql.Rows, on_row func(RowMap) error) error {
	columns, _ := rows.Columns()
	err := ScanRowsToArrays(rows, func(arr []CellData) error {
		m := rowToMap(arr, columns)
		err := on_row(m)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

func QueryRowsMap(db *sql.DB, query string, on_row func(RowMap) error, args ...interface{}) error {
	var err error
	defer func() {
		if derr := recover(); derr != nil {
			err = errors.New(fmt.Sprintf("QueryRowsMap unexpected error: %+v", derr))
		}
	}()

	rows, err := db.Query(query, args...)
	defer rows.Close()
	if err != nil && err != sql.ErrNoRows {
		return log.Errore(err)
	}
	err = ScanRowsToMaps(rows, on_row)
	return err
}

func queryResultData(db *sql.DB, query string, retrieveColumns bool, args ...interface{}) (ResultData, []string, error) {
	var err error
	defer func() {
		if derr := recover(); derr != nil {
			err = errors.New(fmt.Sprintf("QueryRowsMap unexpected error: %+v", derr))
		}
	}()

	columns := []string{}
	rows, err := db.Query(query, args...)
	defer rows.Close()
	if err != nil && err != sql.ErrNoRows {
		return EmptyResultData, columns, log.Errore(err)
	}
	if retrieveColumns {
		columns, _ = rows.Columns()
	}
	resultData := ResultData{}
	err = ScanRowsToArrays(rows, func(rowData []CellData) error {
		resultData = append(resultData, rowData)
		return nil
	})
	return resultData, columns, err
}

func QueryResultData(db *sql.DB, query string, args ...interface{}) (ResultData, error) {
	resultData, _, err := queryResultData(db, query, false, args...)
	return resultData, err
}

func QueryResultDataNamed(db *sql.DB, query string, args ...interface{}) (ResultData, []string, error) {
	return queryResultData(db, query, true, args...)
}

func QueryRowsMapBuffered(db *sql.DB, query string, on_row func(RowMap) error, args ...interface{}) error {
	resultData, columns, err := queryResultData(db, query, true, args...)
	if err != nil {
		return err
	}
	for _, row := range resultData {
		err = on_row(rowToMap(row, columns))
		if err != nil {
			return err
		}
	}
	return nil
}

func ExecNoPrepare(db *sql.DB, query string, args ...interface{}) (sql.Result, error) {
	var err error
	defer func() {
		if derr := recover(); derr != nil {
			err = errors.New(fmt.Sprintf("ExecNoPrepare unexpected error: %+v", derr))
		}
	}()

	var res sql.Result
	res, err = db.Exec(query, args...)
	if err != nil {
		log.Errore(err)
	}
	return res, err
}

func execInternal(silent bool, db *sql.DB, query string, args ...interface{}) (sql.Result, error) {
	var err error
	defer func() {
		if derr := recover(); derr != nil {
			err = errors.New(fmt.Sprintf("execInternal unexpected error: %+v", derr))
		}
	}()

	stmt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	var res sql.Result
	res, err = stmt.Exec(args...)
	if err != nil && !silent {
		log.Errore(err)
	}
	return res, err
}

func Exec(db *sql.DB, query string, args ...interface{}) (sql.Result, error) {
	return execInternal(false, db, query, args...)
}

func ExecSilently(db *sql.DB, query string, args ...interface{}) (sql.Result, error) {
	return execInternal(true, db, query, args...)
}

func InClauseStringValues(terms []string) string {
	quoted := []string{}
	for _, s := range terms {
		quoted = append(quoted, fmt.Sprintf("'%s'", strings.Replace(s, ",", "''", -1)))
	}
	return strings.Join(quoted, ", ")
}

func Args(args ...interface{}) []interface{} {
	return args
}
