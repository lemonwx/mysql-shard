/**
 *  author: lim
 *  data  : 18-10-8 下午8:03
 */

package mysql

type shardTx struct {
}

func (st *shardTx) Commit() error {
	return nil
}

func (st *shardTx) Rollback() error {
	return nil
}
