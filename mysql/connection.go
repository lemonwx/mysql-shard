/**
 *  author: lim
 *  data  : 18-10-8 下午7:15
 */

package mysql

import (
	"database/sql/driver"

	"github.com/go-sql-driver/mysql"
	"github.com/lemonwx/log"

	d "github.com/xelabs/go-mysqlstack/driver"
)

type ShardConn struct {
	cos map[int]d.Conn
}

func (co *ShardConn) Prepare(query string) (driver.Stmt, error) {
	stmt := &shardStmt{}
	return stmt, nil
}

func (co *ShardConn) Close() error {
	for _, back := range co.cos {
		back.Close()
	}
	return nil
}

func (co *ShardConn) Begin() (driver.Tx, error) {
	tx := &shardTx{}
	return tx, nil
}

func (co *ShardConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	log.Debug(query, args)
	rows, err := co.cos[0].Query(query)
	return &shardRows{rows}, err
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
