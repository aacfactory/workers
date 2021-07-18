package workers_test

import (
	"fmt"
	"github.com/aacfactory/workers"
	"runtime"
	"testing"
	"time"
)

func BenchmarkNewWorkers(b *testing.B) {
	option := workers.Option{
		MaxWorkerNum: runtime.NumCPU() * 8,
		MaxIdleTime:  0,
		Fn:           FnB,
	}
	worker := workers.NewWorkers(option)
	worker.Start()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		worker.Command(i, "a", fmt.Sprintf("%d", i))
	}

	worker.Stop()
}

func FnB(unit interface{}, meta map[string]string) {
	time.Sleep(50 * time.Microsecond)
}
