/**
 *  author: lim
 *  data  : 18-10-8 下午7:20
 */

package mysql

import (
	"database/sql/driver"

	"github.com/lemonwx/log"
)

type shardStmt struct {
}

func (ss *shardStmt) Close() error {
	return nil
}

func (ss *shardStmt) NumInput() int {
	return 0
}

func (ss *shardStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, nil
}

func (ss *shardStmt) Query(args []driver.Value) (driver.Rows, error) {
	log.Debug(args)
	return nil, nil
}
