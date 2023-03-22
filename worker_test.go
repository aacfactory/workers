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

package workers_test

import (
	"context"
	"fmt"
	"github.com/aacfactory/workers"
	"io"
	"testing"
	"time"
)

func TestNewWorkers(t *testing.T) {
	worker := workers.New()
	x := 0
	ctx := context.TODO()
	for i := 0; i < 10; i++ {
		ok := worker.Dispatch(ctx, &Task{
			Id: i,
		})
		if ok {
			x++
		}
	}
	worker.Close()
	fmt.Println("ok", x)
}

func TestWorkers_MustDispatch(t *testing.T) {
	ctx := context.TODO()
	worker := workers.New(workers.MaxWorkers(2))
	for i := 0; i < 10; i++ {
		worker.MustDispatch(ctx, &Task{
			Id: i,
		})

	}
	worker.Close()
	fmt.Println("ok")
}

type Task struct {
	Id int
}

func (task *Task) Execute(ctx context.Context) {
	fmt.Println(task.Id)
	time.Sleep(50 * time.Microsecond)
}

type LongTask struct {
	*workers.AbstractLongTask
	closed chan struct{}
}

func (task *LongTask) Execute(ctx context.Context) {
	n := time.Duration(1)
	for {
		fmt.Println("...")
		if aborted, cause := task.Aborted(); aborted {
			fmt.Println("aborted", cause)
			break
		}
		<-time.After(n * time.Second)
		fmt.Println(n)
		n++
		if n > 1 {
			task.Close()
		}
		task.Touch(3 * time.Second)
	}
	close(task.closed)
}

func TestLongTask(t *testing.T) {
	worker := workers.New()
	ctx := context.TODO()
	io.Reader{}.Read()
	closed := make(chan struct{}, 1)
	worker.Dispatch(ctx, &LongTask{
		AbstractLongTask: workers.NewAbstractLongTask(3 * time.Second),
		closed:           closed,
	})
	<-closed
	worker.Close()
}
