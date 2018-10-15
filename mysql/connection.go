/**
 *  author: lim
 *  data  : 18-10-8 下午7:15
 */

package mysql

import (
	"database/sql/driver"

	"fmt"

	"strconv"

	"github.com/go-sql-driver/mysql"
	"github.com/lemonwx/log"
	d "github.com/xelabs/go-mysqlstack/driver"
)

const (
	Shard = "shard"
	Host  = 0
	Port  = 1
)

type shard struct {
	size  uint
	cfg   *mysql.Config
	addrs []string
}

func parseShard(dsn string) (s *shard, err error) {
	var shardEncode string
	var shardSize uint64
	var ok bool

	s.cfg, err = mysql.ParseDSN(dsn)
	if err != nil {
		return
	}

	shardEncode, ok = s.cfg.Params[Shard]
	if !ok {
		err = fmt.Errorf("cant not find shard size")
	}

	shardSize, err = strconv.ParseUint(shardEncode, 10, 64)
	if err != nil {
		err = fmt.Errorf("parse shard size failed: %v", err)
		return
	}

	for idx := uint64(0); idx < shardSize; idx += 1 {
		key := fmt.Sprintf("shard%d", idx)
		addr, ok := s.cfg.Params[key]
		if !ok {
			err = fmt.Errorf("shard size: %d, but can't get key: %s", shardSize, key)
			return
		}
		s.addrs = append(s.addrs, addr)
	}
	return
}

type ShardConn struct {
	cos     map[int]d.Conn
	adminCo d.Conn

	shard *shard
}

func (sc *ShardConn) Close() error {
	for _, back := range sc.cos {
		back.Close()
	}
	return nil
}

func (sc *ShardConn) Begin() (driver.Tx, error) {
	if err := sc.cos[0].Exec("start transaction/*by lim*/"); err != nil {
		return nil, err
	}
	tx := &shardTx{sc}
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
	if sc.shard == nil {
		if sc.shard, err = parseShard(dsn); err != nil {
			return err
		}
	}

	if sc.adminCo, err = d.NewConn(
		sc.shard.cfg.User,
		sc.shard.cfg.Passwd,
		sc.shard.cfg.Passwd,
		sc.shard.cfg.DBName,
		"utf8"); err != nil {
		return err
	}

	return nil
}

func (sc *ShardConn) Prepare(query string) (driver.Stmt, error) {
	stmt := &shardStmt{}
	return stmt, nil
}
