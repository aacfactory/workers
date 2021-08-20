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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkNewWorkers(b *testing.B) {

	worker, workErr := workers.NewWorkers(&BHandler{})
	if workErr != nil {
		b.Error(workErr)
		return
	}
	worker.Start()
	count := int64(0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if worker.Execute("test", i) {
			atomic.AddInt64(&count, 1)
		}
	}

	worker.Stop()
	b.Log(count, b.N)
}

type BHandler struct {
}

func (h *BHandler) Handle(action string, payload interface{}) {
	time.Sleep(50 * time.Microsecond)
}

func BenchmarkChanWorkers(b *testing.B) {
	ch := make(chan interface{}, runtime.NumCPU()*1024)
	wg := &sync.WaitGroup{}
	for i := 0; i < runtime.NumCPU()*2; i++ {
		go func(ch chan interface{}, wg *sync.WaitGroup) {
			for {
				_, ok := <-ch
				if !ok {
					break
				}
				time.Sleep(50 * time.Microsecond)
				wg.Done()
			}
		}(ch, wg)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		ch <- i
	}
	wg.Wait()
	close(ch)

}
