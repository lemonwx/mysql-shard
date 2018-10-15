/**
 *  author: lim
 *  data  : 18-10-15 下午11:16
 */

package executor

import (
	"sync"

	"fmt"

	"github.com/xelabs/go-mysqlstack/driver"
)

type Executor interface {
	Execute(string) (driver.Rows, error)
}

type MultiExecutor struct {
	wg    sync.WaitGroup
	mutex sync.Mutex
	cos   map[int]driver.Conn
}

func (me *MultiExecutor) Execute(query string) (driver.Rows, error) {
	me.wg.Add(len(me.cos))

	rets := make([]driver.Rows, 0, len(me.cos))
	errs := make([]error, 0, len(me.cos))

	for _, co := range me.cos {
		go func(co driver.Conn) {
			defer me.wg.Done()
			rows, err := co.Query(query)
			if err != nil {
				me.mutex.Lock()
				errs = append(errs, err)
				me.mutex.Unlock()
			} else {
				me.mutex.Lock()
				rets = append(rets, rows)
				me.mutex.Unlock()
			}
		}(co)
	}

	me.wg.Wait()

	switch {
	case len(rets) == len(me.cos):
		// todo: merge multi rows
		return rets[0], nil
	case len(errs) == len(me.cos):
		return nil, errs[0]
	default:
		err := fmt.Errorf("unexpected multi nodes response not equal")
		return nil, err
	}
}

func NewExecutor(cos map[int]driver.Conn) Executor {
	return &MultiExecutor{cos: cos, wg: sync.WaitGroup{}, mutex: sync.Mutex{}}
}
