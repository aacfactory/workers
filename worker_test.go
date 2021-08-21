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
	"testing"
	"time"
)

func TestNewWorkers(t *testing.T) {

	worker, workErr := workers.New(&Handler{})
	if workErr != nil {
		t.Error(workErr)
		return
	}
	worker.Start()
	x := 0
	for i := 0; i < 10; i++ {
		ok := worker.Execute("test", i)
		if ok {
			x++
		}
	}
	worker.Stop()
	fmt.Println("ok", x)
}

type Handler struct {
}

func (h *Handler) Handle(action string, payload interface{}) {
	fmt.Println(action, payload)
	time.Sleep(50 * time.Microsecond)
}

func Fn(unit interface{}, meta map[string]string) {
	fmt.Println("do", unit, meta)
	time.Sleep(1 * time.Second)
}

func TestContext(t *testing.T) {
	fmt.Println(context.Background().Done() == nil)
	ctx := context.TODO()
	ctx0, cn := context.WithTimeout(ctx, 10*time.Second)
	fmt.Println(ctx0.Deadline())
	cn()
}
