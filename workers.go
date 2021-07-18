package workers

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type Unit struct {
	v        interface{}
	meta     map[string]string
	accepted chan bool
}

type Execute func(v interface{}, meta map[string]string)

type Option struct {
	MaxWorkerNum      int
	MaxIdleTime       time.Duration
	CommandTimeout    time.Duration
	CommandBufferSize int
	Fn                Execute
}

func NewWorkers(option Option) (w *Workers) {
	maxWorkerNum := option.MaxWorkerNum
	if maxWorkerNum < 1 {
		maxWorkerNum = runtime.NumCPU() * 2
	}
	maxIdleTime := option.MaxIdleTime
	if maxIdleTime <= 0 {
		maxIdleTime = 10 * time.Second
	}
	commandTimeout := option.CommandTimeout
	if commandTimeout <= 0 {
		commandTimeout = 2 * time.Second
	}
	commandBufferSize := option.CommandBufferSize
	if commandBufferSize < 1 {
		commandBufferSize = runtime.NumCPU() * 64
	}
	fn := option.Fn
	if fn == nil {
		panic(fmt.Errorf("create workers failed, fn is nil"))
	}
	w = &Workers{
		fn:                    fn,
		maxWorkersCount:       maxWorkerNum,
		maxIdleWorkerDuration: maxIdleTime,
		lock:                  sync.Mutex{},
		workersCount:          0,
		mustStop:              false,
		commandTimeout:        commandTimeout,
		commandBuffer:         make(chan *Unit, commandBufferSize),
		workerChanPool:        sync.Pool{},
		unitPool:              sync.Pool{},
		wg:                    sync.WaitGroup{},
	}
	return
}

type Workers struct {
	running               int64
	fn                    Execute
	maxWorkersCount       int
	maxIdleWorkerDuration time.Duration
	lock                  sync.Mutex
	workersCount          int
	mustStop              bool
	ready                 []*workerChan
	commandTimeout        time.Duration
	commandBuffer         chan *Unit
	stopCh                chan struct{}
	workerChanPool        sync.Pool
	unitPool              sync.Pool
	wg                    sync.WaitGroup
}

func (w *Workers) Command(v interface{}, metaKvs ...string) (ok bool) {
	if atomic.LoadInt64(&w.running) == int64(0) {
		ok = false
		return
	}
	unit := w.unitPool.Get().(*Unit)
	unit.v = v
	unit.meta = make(map[string]string)
	if metaKvs != nil {
		for i := 0; i < len(metaKvs); i++ {
			k := metaKvs[i]
			i++
			v := metaKvs[i]
			unit.meta[k] = v
		}
	}
	ctx, cancel := context.WithTimeout(context.TODO(), w.commandTimeout)
	select {
	case w.commandBuffer <- unit:
		ok = true
		break
	case <-ctx.Done():
		ok = false
		break
	}
	if !ok {
		unit.v = nil
		if len(unit.meta) != 0 {
			unit.meta = make(map[string]string)
		}
		w.unitPool.Put(unit)
		cancel()
		return
	}
	ok = false
	select {
	case ok = <-unit.accepted:
		if !ok {
			unit.v = nil
			if len(unit.meta) != 0 {
				unit.meta = make(map[string]string)
			}
			w.unitPool.Put(unit)
		}
		break
	case <-ctx.Done():
		ok = false
		break
	}
	cancel()
	return
}

type workerChan struct {
	lastUseTime time.Time
	ch          chan *Unit
}

var workerChanCap = func() int {
	if runtime.GOMAXPROCS(0) == 1 {
		return 0
	}
	return 1
}()

func (w *Workers) Start() {
	if w.stopCh != nil {
		panic("workers is already started")
	}
	w.stopCh = make(chan struct{})
	stopCh := w.stopCh
	w.workerChanPool.New = func() interface{} {
		return &workerChan{
			ch: make(chan *Unit, workerChanCap),
		}
	}
	w.unitPool.New = func() interface{} {
		return &Unit{
			v:        nil,
			meta:     make(map[string]string),
			accepted: make(chan bool, workerChanCap),
		}
	}
	go w.listen()
	go func() {
		var scratch []*workerChan
		for {
			w.clean(&scratch)
			select {
			case <-stopCh:
				return
			default:
				time.Sleep(w.maxIdleWorkerDuration)
			}
		}
	}()
	w.running = 1
}

func (w *Workers) Stop() {
	if w.stopCh == nil {
		panic("workers wasn't started")
	}
	w.running = 0
	close(w.stopCh)
	close(w.commandBuffer)
	w.stopCh = nil

	w.lock.Lock()
	ready := w.ready
	for i := range ready {
		ready[i].ch <- nil
		ready[i] = nil
	}
	w.ready = ready[:0]
	w.mustStop = true
	w.lock.Unlock()
}

func (w *Workers) Sync() {
	w.wg.Wait()
}

func (w *Workers) listen() {
	for {
		unit, ok := <-w.commandBuffer
		if !ok {
			break
		}
		var ch *workerChan = nil
		for i := 0; i < 5; i++ {
			ch = w.getCh()
			if ch == nil {
				time.Sleep(1 * time.Second)
				continue
			}
			break
		}
		if ch == nil {
			unit.accepted <- false
			continue
		}
		w.wg.Add(1)
		unit.accepted <- true
		ch.ch <- unit
	}
}

func (w *Workers) clean(scratch *[]*workerChan) {
	maxIdleWorkerDuration := w.maxIdleWorkerDuration

	criticalTime := time.Now().Add(-maxIdleWorkerDuration)

	w.lock.Lock()
	ready := w.ready
	n := len(ready)

	l, r, mid := 0, n-1, 0
	for l <= r {
		mid = (l + r) / 2
		if criticalTime.After(w.ready[mid].lastUseTime) {
			l = mid + 1
		} else {
			r = mid - 1
		}
	}
	i := r
	if i == -1 {
		w.lock.Unlock()
		return
	}

	*scratch = append((*scratch)[:0], ready[:i+1]...)
	m := copy(ready, ready[i+1:])
	for i = m; i < n; i++ {
		ready[i] = nil
	}
	w.ready = ready[:m]
	w.lock.Unlock()

	tmp := *scratch
	for i := range tmp {
		tmp[i].ch <- nil
		tmp[i] = nil
	}
}

func (w *Workers) getCh() *workerChan {
	var ch *workerChan
	createWorker := false

	w.lock.Lock()
	ready := w.ready
	n := len(ready) - 1
	if n < 0 {
		if w.workersCount < w.maxWorkersCount {
			createWorker = true
			w.workersCount++
		}
	} else {
		ch = ready[n]
		ready[n] = nil
		w.ready = ready[:n]
	}
	w.lock.Unlock()

	if ch == nil {
		if !createWorker {
			return nil
		}
		vch := w.workerChanPool.Get()
		ch = vch.(*workerChan)
		go func() {
			w.work(ch)
			w.workerChanPool.Put(vch)
		}()
	}
	return ch
}

func (w *Workers) release(ch *workerChan) bool {
	ch.lastUseTime = time.Now()
	w.lock.Lock()
	if w.mustStop {
		w.lock.Unlock()
		return false
	}
	w.ready = append(w.ready, ch)
	w.lock.Unlock()
	return true
}

func (w *Workers) work(ch *workerChan) {
	var unit *Unit

	for unit = range ch.ch {
		if unit == nil {
			break
		}

		w.fn(unit.v, unit.meta)
		w.wg.Done()
		unit.v = nil
		if len(unit.meta) != 0 {
			unit.meta = make(map[string]string)
		}

		w.unitPool.Put(unit)

		if !w.release(ch) {
			break
		}
	}

	w.lock.Lock()
	w.workersCount--
	w.lock.Unlock()
}
