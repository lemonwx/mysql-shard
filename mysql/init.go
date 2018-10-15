/**
 *  author: lim
 *  data  : 18-10-8 下午7:04
 */

package mysql

import (
	"database/sql"
	"database/sql/driver"

	d "github.com/xelabs/go-mysqlstack/driver"
)

type ShardDriver struct{}

func (sd ShardDriver) Open(dsn string) (driver.Conn, error) {
	sc := &ShardConn{cos: map[int]d.Conn{}}
	if err := sc.Connect(dsn); err != nil {
		return nil, err
	}
	return sc, nil
}

func init() {
	sql.Register("mysql-shard", &ShardDriver{})
}
