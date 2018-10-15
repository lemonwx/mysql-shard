/**
 *  author: lim
 *  data  : 18-10-8 下午8:39
 */

package mysql

import (
	"database/sql/driver"

	"fmt"

	"io"

	d "github.com/xelabs/go-mysqlstack/driver"
)

type shardRows struct {
	d.Rows
}

func (sr *shardRows) Columns() []string {
	ret := []string{}
	for _, f := range sr.Fields() {
		ret = append(ret, f.Name)
	}

	return ret
}

func (sr *shardRows) Next(dest []driver.Value) error {
	if !sr.Rows.Next() {
		return io.EOF
	}
	vals, err := sr.RowValues()
	if err != nil {
		return err
	}
	if len(dest) != len(vals) {
		return fmt.Errorf("dest and src not equal")
	}

	for idx := 0; idx < len(dest); idx += 1 {
		dest[idx] = vals[idx].String()
	}

	return nil
}

type shardResult struct {
	d.Rows
}

func (sr *shardResult) LastInsertId() (int64, error) {
	return int64(sr.Rows.LastInsertID()), nil
}

func (sr *shardResult) RowsAffected() (int64, error) {
	return int64(sr.Rows.RowsAffected()), nil
}
