/**
 *  author: lim
 *  data  : 18-10-11 下午10:55
 */

package basic

import (
	"database/sql"

	"fmt"
	"time"

	"github.com/lemonwx/log"
)

func TestConnect(url, driverName string) {
	db, err := sql.Open(driverName, url)
	log.Debugf("connect db: %v, err: %v", db, err)
}

func TestQuery(url, driverName string) {
	db, err := sql.Open(driverName, url)
	if err != nil {
		log.Errorf("run test query, OpenDb failed: %v", err)
		return
	}

	rows, err := db.Query("select * from tb")
	if err != nil {
		log.Errorf("run test query, run Query failed: %v", err)
		return
	}

	var v uint64
	var id int
	var name string

	for rows.Next() {
		if err := rows.Scan(&v, &id, &name); err != nil {
			log.Errorf("run test query, run Scan failed: %v", err)
			return
		}
		log.Debugf("scan ret => v: %d, id: %d, name: %s", v, id, name)
	}
	db.Close()
}

func TestExec(url, driverName string) {
	db, err := sql.Open(driverName, url)
	if err != nil {
		log.Errorf("run test exec, OpenDb failed: %v", err)
		return
	}

	ret, err := db.Exec(fmt.Sprintf("update tb set name = '%v'", time.Now().Second()))
	if err != nil {
		log.Errorf("run exec failed: %v", err)
		return
	}
	aft, err := ret.RowsAffected()
	if err != nil {
		log.Errorf("get rows aft failed: %v", err)
		return
	}

	lst, err := ret.LastInsertId()
	if err != nil {
		log.Errorf("get last ist id failed: %v", err)
		return
	}

	log.Debug(aft)
	log.Debug(lst)

	db.Close()
}
