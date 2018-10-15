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
	"github.com/lemonwx/shard-driver/executor"
	"github.com/lemonwx/shard-driver/router"
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
	s = &shard{}
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

	shard  *shard
	status uint16
}

func (sc *ShardConn) getCos(query string) (map[int]d.Conn, error) {
	var shardList []int
	var err error
	cos := map[int]d.Conn{}

	if shardList, err = router.GetShardList(); err != nil {
		return nil, err
	}

	for _, idx := range shardList {
		if !(idx < len(sc.shard.addrs)) {
			err = fmt.Errorf("error: you wanted co index: %d out of range", idx)
			return nil, err
		}

		co, ok := sc.cos[idx]
		if ok {
			cos[idx] = co
			continue
		}

		sc.cos[idx], err = d.NewConn(
			sc.shard.cfg.User,
			sc.shard.cfg.Passwd,
			sc.shard.addrs[idx],
			sc.shard.cfg.DBName,
			"utf8",
		)

		if err != nil {
			return nil, err
		}
		cos[idx] = sc.cos[idx]
	}

	return cos, nil
}

func (sc *ShardConn) Close() error {
	sc.adminCo.Close()

	log.Debug(sc.cos)
	for _, back := range sc.cos {
		back.Close()
	}
	return nil
}

func (sc *ShardConn) Begin() (driver.Tx, error) {
	tx := &shardTx{sc}
	return tx, nil
}

func (sc *ShardConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	var cos map[int]d.Conn
	var rows d.Rows
	var err error

	if cos, err = sc.getCos(query); err != nil {
		return nil, err
	}

	exec := executor.NewExecutor(cos)
	if rows, err = exec.Execute(query); err != nil {
		return nil, err
	}

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
		sc.shard.cfg.Addr,
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
