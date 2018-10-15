/**
 *  author: lim
 *  data  : 18-10-8 下午7:15
 */

package mysql

import (
	"database/sql/driver"

	"fmt"

	"github.com/go-sql-driver/mysql"
	"github.com/lemonwx/log"
	d "github.com/xelabs/go-mysqlstack/driver"
)

type ShardConn struct {
	cos map[int]d.Conn
}

func (sc *ShardConn) Close() error {
	for _, back := range sc.cos {
		back.Close()
	}
	return nil
}

func (sc *ShardConn) Begin() (driver.Tx, error) {
	tx := &shardTx{}
	return tx, nil
}

func (sc *ShardConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	log.Debug(query, args)
	rows, err := sc.cos[0].Query(query)
	return &shardRows{rows}, err
}

func (sc *ShardConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if len(args) != 0 {
		return nil, fmt.Errorf("unsupported prepare stmt")
	}

	rows, err := sc.cos[0].Query(query)
	if err != nil {
		return nil, err
	}
	return &shardResult{rows}, nil
}

func (sc *ShardConn) Connect(dsn string) error {
	var err error
	var cfg *mysql.Config
	var conn d.Conn

	if cfg, err = mysql.ParseDSN(dsn); err != nil {
		return err
	}

	if conn, err = d.NewConn(cfg.User, cfg.Passwd, cfg.Addr, cfg.DBName, "utf8"); err != nil {
		return err
	}

	sc.cos[0] = conn
	return nil
}

func (sc *ShardConn) Prepare(query string) (driver.Stmt, error) {
	stmt := &shardStmt{}
	return stmt, nil
}
