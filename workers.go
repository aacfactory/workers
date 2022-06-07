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
	defaultConcurrency           = 256 * 1024
	defaultWorkerMaxIdleDuration = 2 * time.Second
)

type Option func(*Options) error

type Options struct {
	Concurrency           int
	WorkerMaxIdleDuration time.Duration
}

func Concurrency(concurrency int) Option {
	return func(o *Options) error {
		if concurrency < 1 {
			return fmt.Errorf("concurrency must great than 0")
		}
		o.Concurrency = concurrency
		return nil
	}
}

func WorkerMaxIdleDuration(d time.Duration) Option {
	return func(o *Options) error {
		if d < 1 {
			return fmt.Errorf("max idle duration must great than 0")
		}
		o.WorkerMaxIdleDuration = d
		return nil
	}
}

func New(handler Handler, options ...Option) (w Workers) {
	if handler == nil {
		panic(fmt.Errorf("new workers failed for handler is nil"))
		return
	}
	opt := &Options{
		Concurrency:           defaultConcurrency,
		WorkerMaxIdleDuration: defaultWorkerMaxIdleDuration,
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
		maxWorkersCount:       int64(opt.Concurrency),
		maxIdleWorkerDuration: opt.WorkerMaxIdleDuration,
		lock:                  sync.Mutex{},
		workersCount:          0,
		mustStop:              false,
		ready:                 nil,
		stopCh:                nil,
		workerChanPool:        sync.Pool{},
		handler:               handler,
	}
	ws.serve()
	w = ws
	return
}

type Workers interface {
	Execute(command interface{}) (ok bool)
	Close()
}

type Handler interface {
	Handle(payload interface{})
}

type workerChan struct {
	lastUseTime time.Time
	ch          chan interface{}
}

type workers struct {
	maxWorkersCount       int64
	maxIdleWorkerDuration time.Duration
	lock                  sync.Mutex
	workersCount          int64
	mustStop              bool
	ready                 []*workerChan
	stopCh                chan struct{}
	workerChanPool        sync.Pool
	handler               Handler
}

func (w *workers) Execute(command interface{}) (ok bool) {
	ch := w.getReady()
	if ch == nil {
		return false
	}
	ch.ch <- command
	return true
}

func (w *workers) Close() {
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
}

func (w *workers) serve() {
	w.stopCh = make(chan struct{})
	stopCh := w.stopCh
	w.workerChanPool.New = func() interface{} {
		return &workerChan{
			ch: make(chan interface{}, 1),
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
		command, ok := <-wch.ch
		if !ok {
			break
		}
		w.handler.Handle(command)
		if !w.release(wch) {
			break
		}
	}
	w.lock.Lock()
	w.workersCount--
	w.lock.Unlock()
}
