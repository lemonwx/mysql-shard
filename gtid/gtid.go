/**
 *  author: lim
 *  data  : 18-10-8 下午9:04
 */

package gtid

type Gtid interface {
	Get() (map[uint64]bool, error)
	Next() (uint64, error)
	Release(gtid uint64) error
	GetAndNext() (uint64, map[uint64]bool, error)
}

type LocalGtid struct {
	actives map[uint64]bool
	next    uint64
}

func (lg *LocalGtid) Get() (map[uint64]bool, error) {
	return lg.actives, nil
}

func (lg *LocalGtid) Next() (uint64, error) {
	lg.next += 1
	return lg.next, nil
}

func (lg *LocalGtid) Release(gtid uint64) error {
	delete(lg.actives, gtid)
	return nil
}

func (lg *LocalGtid) GetAndNext() (uint64, map[uint64]bool, error) {
	lg.next += 1
	return lg.next, lg.actives, nil
}
