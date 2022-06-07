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
	"sync/atomic"
	"time"
)

const (
	defaultMaxWorkers            = 256 * 1024
	defaultMaxIdleWorkerDuration = 2 * time.Second
)

type Option func(*Options) error

type Options struct {
	MaxWorkers            int
	MaxIdleWorkerDuration time.Duration
}

func MaxWorkers(max int) Option {
	return func(o *Options) error {
		if max < 1 {
			return fmt.Errorf("max workers must great than 0")
		}
		o.MaxWorkers = max
		return nil
	}
}

func MaxIdleWorkerDuration(d time.Duration) Option {
	return func(o *Options) error {
		if d < 1 {
			return fmt.Errorf("max idle duration must great than 0")
		}
		o.MaxIdleWorkerDuration = d
		return nil
	}
}

func New(options ...Option) (w Workers) {
	opt := &Options{
		MaxWorkers:            defaultMaxWorkers,
		MaxIdleWorkerDuration: defaultMaxIdleWorkerDuration,
	}
	if options != nil {
		for _, option := range options {
			optErr := option(opt)
			if optErr != nil {
				panic(fmt.Errorf("new workers failed, %v", optErr))
				return
			}
		}
	}
	ws := &workers{
		maxWorkersCount:       int64(opt.MaxWorkers),
		maxIdleWorkerDuration: opt.MaxIdleWorkerDuration,
		lock:                  sync.Mutex{},
		workersCount:          0,
		running:               0,
		mustStop:              false,
		ready:                 nil,
		delegates:             nil,
		stopDelegatesCh:       nil,
		stopCh:                nil,
		workerChanPool:        sync.Pool{},
	}
	ws.serve()
	w = ws
	return
}

type Task interface {
	Execute()
}

type Workers interface {
	Dispatch(task Task) (ok bool)
	MustDispatch(task Task)
	Close()
}

type workerChan struct {
	lastUseTime time.Time
	ch          chan Task
}

type workers struct {
	maxWorkersCount       int64
	maxIdleWorkerDuration time.Duration
	lock                  sync.Mutex
	workersCount          int64
	running               int64
	mustStop              bool
	ready                 []*workerChan
	delegates             chan Task
	stopDelegatesCh       chan struct{}
	stopCh                chan struct{}
	workerChanPool        sync.Pool
}

func (w *workers) Dispatch(task Task) (ok bool) {
	if task == nil || atomic.LoadInt64(&w.running) == 0 {
		return false
	}
	ch := w.getReady()
	if ch == nil {
		return false
	}
	ch.ch <- task
	return true
}

func (w *workers) MustDispatch(task Task) {
	if task == nil || atomic.LoadInt64(&w.running) == 0 {
		return
	}
	w.delegates <- task
}

func (w *workers) Close() {
	atomic.StoreInt64(&w.running, 0)
	close(w.stopCh)
	close(w.stopDelegatesCh)
	w.stopCh = nil
	w.stopDelegatesCh = nil
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

func (w *workers) serve() {
	w.running = 1
	w.stopCh = make(chan struct{})
	stopCh := w.stopCh
	w.stopDelegatesCh = make(chan struct{})
	stopDelegatesCh := w.stopDelegatesCh
	w.delegates = make(chan Task, w.maxWorkersCount)
	delegates := w.delegates
	w.workerChanPool.New = func() interface{} {
		return &workerChan{
			ch: make(chan Task, 1),
		}
	}
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
	go func() {
		for {
			select {
			case task := <-delegates:
				for {
					if w.Dispatch(task) {
						break
					}
					time.Sleep(50 * time.Millisecond)
				}
			case <-stopDelegatesCh:
				return
			}
		}
	}()
}

func (w *workers) clean(scratch *[]*workerChan) {
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

func (w *workers) getReady() *workerChan {
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
			w.handle(ch)
			w.workerChanPool.Put(vch)
		}()
	}
	return ch
}

func (w *workers) release(ch *workerChan) bool {
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

func (w *workers) handle(wch *workerChan) {
	for {
		task := <-wch.ch
		if task == nil {
			break
		}
		task.Execute()
		if !w.release(wch) {
			break
		}
	}
	w.lock.Lock()
	w.workersCount--
	w.lock.Unlock()
}
