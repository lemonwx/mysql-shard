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

var db *sql.DB

const (
	Count = 10
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

func TestDel(db *sql.DB) error {
	ret, err := db.Exec("delete from tb")
	if err != nil {
		log.Errorf("run del failed: %v", err)
		return err
	}

	aft, err := ret.RowsAffected()
	if err != nil {
		log.Errorf("get rows aft failed: %v", err)
		return err
	}

	log.Debug(aft)
	return nil
}

func TestIst(db *sql.DB) error {
	for idx := 0; idx < Count; idx += 1 {
		ret, err := db.Exec(fmt.Sprintf("insert into tb(id, name) values (%d, 'im idx:%d')", idx, idx))
		if err != nil {
			log.Errorf("run exec ist failed: %v", err)
			return err
		}

		aft, err := ret.RowsAffected()
		if err != nil {
			log.Errorf("get rows aft failed: %v", err)
			return err
		}

		if aft != 1 {
			err = fmt.Errorf("run ist aft not 1 but %d", aft)
			log.Error(err)
			return err
		}
	}
	return nil
}

func TestUpd(db *sql.DB) error {
	for idx := 0; idx < Count; idx += 1 {
		ret, err := db.Exec(fmt.Sprintf("update tb set name = %v where id = %d", time.Now().Second(), idx))
		if err != nil {
			log.Errorf("run update failed: %v", err)
			return err
		}

		aft, err := ret.RowsAffected()
		if err != nil {
			log.Errorf("get rows aft failed: %v", err)
			return err
		}

		if aft != 1 {
			err = fmt.Errorf("getted rows aft not 1 but %d", aft)
			log.Error(err)
			return err
		}
	}

	return nil
}

func TestExec(url, driverName string) {
	db, err := sql.Open(driverName, url)
	if err != nil {
		log.Errorf("run test exec, OpenDb failed: %v", err)
		return
	}

	if err := TestDel(db); err != nil {
		return
	}

	if err := TestIst(db); err != nil {
		return
	}

	if err := TestUpd(db); err != nil {
		return
	}

	if err := TestDel(db); err != nil {
		return
	}

	db.Close()
}

func TestDelByTx(tx *sql.Tx) error {
	ret, err := tx.Exec("delete from tb")
	if err != nil {
		log.Errorf("run del failed: %v", err)
		return err
	}

	aft, err := ret.RowsAffected()
	if err != nil {
		log.Errorf("get rows aft failed: %v", err)
		return err
	}

	log.Debug(aft)
	return nil
}

func TestUpdByTx(tx *sql.Tx) error {
	for idx := 0; idx < Count; idx += 1 {
		ret, err := tx.Exec(fmt.Sprintf("update tb set name = %v where id = %d", time.Now().Second(), idx))
		if err != nil {
			log.Errorf("run update failed: %v", err)
			return err
		}

		aft, err := ret.RowsAffected()
		if err != nil {
			log.Errorf("get rows aft failed: %v", err)
			return err
		}

		if aft != 1 {
			err = fmt.Errorf("getted rows aft not 1 but %d", aft)
			log.Error(err)
			return err
		}
	}

	return nil
}

func TestIstBtTx(tx *sql.Tx) error {
	for idx := 0; idx < Count; idx += 1 {
		ret, err := tx.Exec(fmt.Sprintf("insert into tb(id, name) values (%d, 'im idx:%d')", idx, idx))
		if err != nil {
			log.Errorf("run exec ist failed: %v", err)
			return err
		}

		aft, err := ret.RowsAffected()
		if err != nil {
			log.Errorf("get rows aft failed: %v", err)
			return err
		}

		if aft != 1 {
			err = fmt.Errorf("run ist aft not 1 but %d", aft)
			log.Error(err)
			return err
		}
	}
	return nil
}

func TestTx(url, driverName string) {
	db, err := sql.Open(driverName, url)
	if err != nil {
		log.Errorf("run test exec, OpenDb failed: %v", err)
		return
	}

	tx, err := db.Begin()
	if err != nil {
		log.Errorf("begin failed: %v", err)
		return
	}

	if err := TestDelByTx(tx); err != nil {
		tx.Rollback()
		return
	}

	if err := TestIstBtTx(tx); err != nil {
		tx.Rollback()
		return
	}

	if err := TestUpdByTx(tx); err != nil {
		tx.Rollback()
		return
	}

	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return
	}

	log.Debug(tx)
}
