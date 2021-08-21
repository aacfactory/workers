/*
 * Copyright 2021 Wang Min Xiang
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package workers

import (
	"fmt"
	"sync"
	"time"
)

const (
	DefaultConcurrency = 256 * 1024
	DefaultMaxIdleTime = 2 * time.Second
)

type Option func(*Options) error

type Options struct {
	Concurrency int
	MaxIdleTime time.Duration
}

func WithConcurrency(concurrency int) Option {
	return func(o *Options) error {
		if concurrency < 1 {
			return fmt.Errorf("concurrency must gt 0")
		}
		o.Concurrency = concurrency
		return nil
	}
}

func WithMaxIdleTime(maxIdleTime time.Duration) Option {
	return func(o *Options) error {
		if maxIdleTime < 1 {
			return fmt.Errorf("maxIdleTime must gt 0")
		}
		o.MaxIdleTime = maxIdleTime
		return nil
	}
}

func New(handler UnitHandler, options ...Option) (w Workers, err error) {
	if handler == nil {
		err = fmt.Errorf("create workers failed for empty handler")
		return
	}
	opt := &Options{
		Concurrency: DefaultConcurrency,
		MaxIdleTime: DefaultMaxIdleTime,
	}
	if options != nil {
		for _, option := range options {
			optErr := option(opt)
			if optErr != nil {
				err = optErr
				return
			}
		}
	}

	w = &workers{
		maxWorkersCount:       opt.Concurrency,
		maxIdleWorkerDuration: opt.MaxIdleTime,
		lock:                  sync.Mutex{},
		workersCount:          0,
		mustStop:              false,
		ready:                 nil,
		stopCh:                nil,
		workerChanPool:        sync.Pool{},
		unitPool:              sync.Pool{},
		unitHandler:           handler,
		wg:                    sync.WaitGroup{},
	}
	return
}

type Workers interface {
	Execute(action string, payload interface{}) (ok bool)
	Start()
	Stop()
}

type unit struct {
	action  string
	payload interface{}
}

type UnitHandler interface {
	Handle(action string, payload interface{})
}

type workerUnitFnChan struct {
	lastUseTime time.Time
	ch          chan *unit
}

type workers struct {
	maxWorkersCount       int
	maxIdleWorkerDuration time.Duration
	lock                  sync.Mutex
	workersCount          int
	mustStop              bool
	ready                 []*workerUnitFnChan
	stopCh                chan struct{}
	workerChanPool        sync.Pool
	unitPool              sync.Pool
	unitHandler           UnitHandler
	wg                    sync.WaitGroup
}

func (w *workers) Execute(action string, payload interface{}) (ok bool) {
	if action == "" {
		return
	}
	ch := w.getCh()
	if ch == nil {
		return false
	}
	w.wg.Add(1)
	u := w.unitPool.Get().(*unit)
	u.action = action
	u.payload = payload
	ch.ch <- u
	return true
}

func (w *workers) Start() {
	if w.stopCh != nil {
		panic("workers is already started")
	}
	w.stopCh = make(chan struct{})
	stopCh := w.stopCh
	w.workerChanPool.New = func() interface{} {
		return &workerUnitFnChan{
			ch: make(chan *unit, 1),
		}
	}
	w.unitPool.New = func() interface{} {
		return &unit{}
	}
	go func() {
		var scratch []*workerUnitFnChan
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
}

func (w *workers) Stop() {
	if w.stopCh == nil {
		panic("workers wasn't started")
	}
	close(w.stopCh)
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
	w.wg.Wait()
}

func (w *workers) clean(scratch *[]*workerUnitFnChan) {
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

func (w *workers) getCh() *workerUnitFnChan {
	var ch *workerUnitFnChan
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
		ch = vch.(*workerUnitFnChan)
		go func() {
			w.handle(ch)
			w.workerChanPool.Put(vch)
		}()
	}
	return ch
}

func (w *workers) release(ch *workerUnitFnChan) bool {
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

func (w *workers) handle(ch *workerUnitFnChan) {
	var u *unit

	for u = range ch.ch {
		if u == nil {
			break
		}
		w.unitHandler.Handle(u.action, u.payload)
		w.unitPool.Put(u)
		w.wg.Done()
		if !w.release(ch) {
			break
		}
	}

	w.lock.Lock()
	w.workersCount--
	w.lock.Unlock()
}
