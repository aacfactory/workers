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
	"fmt"
	"github.com/aacfactory/workers"
	"testing"
	"time"
)

func TestNewWorkers(t *testing.T) {
	worker := workers.New(&Handler{})
	x := 0
	for i := 0; i < 10; i++ {
		ok := worker.Execute(i)
		if ok {
			x++
		}
	}
	worker.Close()
	fmt.Println("ok", x)
}

type Handler struct{}

func (h *Handler) Handle(payload interface{}) {
	fmt.Println(payload)
	time.Sleep(50 * time.Microsecond)
}
