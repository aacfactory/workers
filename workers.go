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
	"context"
	"errors"
	"fmt"
	"runtime"
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
		stopCh:                nil,
		workerChanPool:        sync.Pool{},
	}
	ws.serve()
	w = ws
	return
}

type Task interface {
	Execute(ctx context.Context)
}

type NamedTask interface {
	Name() (name string)
	Task
}

var (
	ErrNoTimeoutInLongTask   = errors.New("no timeout")
	ErrNoHeartbeatInLongTask = errors.New("no heartbeat")
	ErrLongTaskTimeout       = errors.New("timeout")
	ErrLongTaskNormalClosed  = errors.New("normal closed")
)

type LongTask interface {
	Task
	Heartbeat() (initialTimeout time.Duration, ch <-chan time.Duration)
	OnAbort(cause error)
}

func NewAbstractLongTask(initialTimeout time.Duration) *AbstractLongTask {
	return &AbstractLongTask{
		locker:         sync.RWMutex{},
		initialTimeout: initialTimeout,
		heartbeat:      make(chan time.Duration, 2),
	}
}

type AbstractLongTask struct {
	locker         sync.RWMutex
	initialTimeout time.Duration
	heartbeat      chan time.Duration
	aborted        bool
	abortedCause   error
}

func (task *AbstractLongTask) Heartbeat() (initialTimeout time.Duration, ch <-chan time.Duration) {
	initialTimeout = task.initialTimeout
	ch = task.heartbeat
	return
}

func (task *AbstractLongTask) OnAbort(cause error) {
	task.locker.Lock()
	task.locker.Unlock()
	task.aborted = true
	task.abortedCause = cause
	return
}

func (task *AbstractLongTask) Aborted() (ok bool, cause error) {
	task.locker.RLock()
	defer task.locker.RUnlock()
	ok = task.aborted
	cause = task.abortedCause
	return
}

func (task *AbstractLongTask) Touch(nextAliveTimeout time.Duration) {
	task.locker.Lock()
	task.locker.Unlock()
	if task.aborted {
		return
	}
	task.heartbeat <- nextAliveTimeout
}

func (task *AbstractLongTask) Close() {
	task.OnAbort(ErrLongTaskNormalClosed)
	close(task.heartbeat)
}

type Workers interface {
	Dispatch(ctx context.Context, task Task) (ok bool)
	MustDispatch(ctx context.Context, task Task)
	Group() (group Group)
	Close()
}

type workerChan struct {
	lastUseTime time.Time
	ch          chan *taskUnit
}

type taskUnit struct {
	ctx  context.Context
	task Task
}

func (unit *taskUnit) execute() {
	longTask, isLongTask := unit.task.(LongTask)
	if isLongTask {
		initialTimeout, heartbeat := longTask.Heartbeat()
		if initialTimeout < 1 {
			longTask.OnAbort(ErrNoTimeoutInLongTask)
			return
		}
		if heartbeat == nil {
			longTask.OnAbort(ErrNoHeartbeatInLongTask)
			return
		}
		heartbeatTimeout := initialTimeout
		heartbeatTimeoutTimer := time.NewTimer(heartbeatTimeout)
		go longTask.Execute(unit.ctx)
		stop := false
		for {
			select {
			case <-heartbeatTimeoutTimer.C:
				longTask.OnAbort(ErrLongTaskTimeout)
				stop = true
				break
			case nextTimeout, ok := <-heartbeat:
				if !ok {
					stop = true
					break
				}
				heartbeatTimeoutTimer.Reset(nextTimeout)
				break
			}
			if stop {
				break
			}
		}
		heartbeatTimeoutTimer.Stop()
	} else {
		unit.task.Execute(unit.ctx)
	}
}

type workers struct {
	maxWorkersCount       int64
	maxIdleWorkerDuration time.Duration
	lock                  sync.Mutex
	workersCount          int64
	running               int64
	mustStop              bool
	ready                 []*workerChan
	stopCh                chan struct{}
	workerChanPool        sync.Pool
}

func (w *workers) Dispatch(ctx context.Context, task Task) (ok bool) {
	if task == nil || atomic.LoadInt64(&w.running) == 0 {
		return false
	}
	ch := w.getReady()
	if ch == nil {
		return false
	}
	unit := &taskUnit{
		ctx:  ctx,
		task: task,
	}
	select {
	case ch.ch <- unit:
		ok = true
		break
	case <-ctx.Done():
		break
	}
	return
}

func (w *workers) MustDispatch(ctx context.Context, task Task) {
	if task == nil || atomic.LoadInt64(&w.running) == 0 {
		return
	}
	times := 0
	for {
		ok := w.Dispatch(ctx, task)
		if ok {
			break
		}
		deadline, hasDeadline := ctx.Deadline()
		if hasDeadline && deadline.Before(time.Now()) {
			break
		}
		times++
		if times > 9 {
			times = 0
			runtime.Gosched()
		}
	}
}

func (w *workers) Group() (g Group) {
	g = &group{
		ws:    w,
		tasks: make(map[string]GroupTask),
	}
	return
}

func (w *workers) Close() {
	atomic.StoreInt64(&w.running, 0)
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
	w.running = 1
	w.stopCh = make(chan struct{})
	stopCh := w.stopCh
	w.workerChanPool.New = func() interface{} {
		return &workerChan{
			ch: make(chan *taskUnit, 1),
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
		unit := <-wch.ch
		if unit == nil {
			break
		}
		unit.execute()
		if !w.release(wch) {
			break
		}
	}
	w.lock.Lock()
	w.workersCount--
	w.lock.Unlock()
}
