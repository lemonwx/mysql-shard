/**
 *  author: lim
 *  data  : 18-10-8 下午8:03
 */

package mysql

type shardTx struct {
	sc *ShardConn
}

func (st *shardTx) Commit() error {
	if err := st.sc.cos[0].Exec("commit /*by lim*/"); err != nil {
		return err
	}
	return nil
}

func (st *shardTx) Rollback() error {
	return nil
}
