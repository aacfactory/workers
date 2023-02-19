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
	"fmt"
)

var (
	TimeoutErr = fmt.Errorf("workers execute timeout")
)

type GroupTask interface {
	Execute(ctx context.Context) (result interface{}, err error)
}

type groupTask struct {
	key  string
	task GroupTask
	ch   chan *groupTaskResult
}

func (t *groupTask) Execute(ctx context.Context) {
	defer func() {
		_ = recover()
	}()
	result, err := t.task.Execute(ctx)
	t.ch <- &groupTaskResult{
		name:   t.key,
		result: result,
		err:    err,
	}
}

type GroupResult interface {
	Succeed() (ok bool)
	Results() (results map[string]interface{})
	Errors() (err map[string]error)
}

type groupResult struct {
	results map[string]interface{}
	errors  map[string]error
}

func (r *groupResult) Succeed() (ok bool) {
	ok = r.errors == nil || len(r.errors) == 0
	return
}

func (r *groupResult) Results() (results map[string]interface{}) {
	results = r.results
	return
}

func (r *groupResult) Errors() (err map[string]error) {
	err = r.errors
	return
}

type GroupFuture interface {
	Wait(ctx context.Context) (result GroupResult, err error)
}

type groupTaskResult struct {
	name   string
	result interface{}
	err    error
}

type groupFuture struct {
	promises int
	ch       chan *groupTaskResult
}

func (future *groupFuture) Wait(ctx context.Context) (result GroupResult, err error) {
	r := &groupResult{
		results: make(map[string]interface{}),
		errors:  make(map[string]error),
	}
	for {
		stop := false
		select {
		case <-ctx.Done():
			err = TimeoutErr
			stop = true
			break
		case v, ok := <-future.ch:
			if !ok {
				stop = true
				break
			}
			if v.err != nil {
				r.errors[v.name] = v.err
			} else {
				r.results[v.name] = v.result
			}
			future.promises--
			if future.promises < 1 {
				stop = true
				break
			}
		}
		if stop {
			break
		}
	}
	if err == nil {
		result = r
	}
	close(future.ch)
	return
}

type Group interface {
	Add(key string, task GroupTask)
	Run(ctx context.Context) (future GroupFuture)
}

type group struct {
	ws    Workers
	tasks map[string]GroupTask
}

func (g *group) Add(key string, task GroupTask) {
	if key == "" || task == nil {
		return
	}
	g.tasks[key] = task
}

func (g *group) Run(ctx context.Context) (future GroupFuture) {
	f := &groupFuture{
		promises: len(g.tasks),
		ch:       make(chan *groupTaskResult, len(g.tasks)),
	}
	if len(g.tasks) == 0 {
		close(f.ch)
		return
	}
	for key, task := range g.tasks {
		ok := g.ws.Dispatch(ctx, &groupTask{
			key:  key,
			task: task,
			ch:   f.ch,
		})
		if !ok {
			f.ch <- &groupTaskResult{
				name:   key,
				result: nil,
				err:    TimeoutErr,
			}
		}
	}
	future = f
	return
}
