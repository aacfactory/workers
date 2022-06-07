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
	"github.com/aacfactory/workers"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkNewWorkers(b *testing.B) {
	worker := workers.New(&BHandler{}, workers.Concurrency(1024*32*8))
	count := int64(0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if worker.Execute(i) {
			atomic.AddInt64(&count, 1)
		}
	}
	worker.Close()
	b.Log("total", b.N, "accepted", count)
}

type BHandler struct{}

func (h *BHandler) Handle(_ interface{}) {
	time.Sleep(50 * time.Millisecond)
}
