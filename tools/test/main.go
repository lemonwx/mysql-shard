/**
 *  author: lim
 *  data  : 18-10-11 下午10:55
 */

package main

import (
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/lemonwx/log"
	_ "github.com/lemonwx/shard-driver/mysql"
	"github.com/lemonwx/shard-driver/tools/test/basic"
)

var url = "root:root@tcp(172.17.0.2:5518)/db"

func main() {
	log.NewDefaultLogger(os.Stdout)
	log.SetLevel(log.DEBUG)

	for _, driverName := range []string{"mysql-shard", "mysql"} {
		basic.TestConnect(url, driverName)

		basic.TestQuery(url, driverName)
	}
}
