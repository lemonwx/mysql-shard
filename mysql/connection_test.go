/**
 *  author: lim
 *  data  : 18-10-11 下午10:36
 */

package mysql

import (
	"testing"

	"github.com/xelabs/go-mysqlstack/driver"
)

var (
	url = "root:root@tcp(172.17.0.2:5518)/db"
)

func Init() (*ShardConn, error) {
	sc := &ShardConn{cos: map[int]driver.Conn{}}
	if err := sc.Connect(url); err != nil {
		return nil, err
	}
	return sc, nil
}

func TestShardConn_Connect(t *testing.T) {
	sc, err := Init()
	if err != nil {
		t.Error(err)
	}

	sc.Close()
}

func TestShardConn_Query(t *testing.T) {
	sc, err := Init()
	if err != nil {
		t.Error(err)
	}

	rows, err := sc.Query("select * from tb", nil)
	if err != nil {
		t.Error(err)
	}

	t.Log(rows.Columns())
}

func TestShardConn_QueryContext(t *testing.T) {
	sc, err := Init()
	if err != nil {
		t.Error(err)
	}

	rows, err := sc.Query("select * from tb", nil)
	if err != nil {
		t.Error(err)
	}

	t.Log(rows.Columns())
}
