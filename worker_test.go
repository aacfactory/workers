package workers_test

import (
	"fmt"
	"github.com/aacfactory/workers"
	"testing"
	"time"
)

func TestNewWorkers(t *testing.T) {
	option := workers.Option{
		MaxWorkerNum: 0,
		MaxIdleTime:  0,
		Fn:           Fn,
	}
	worker := workers.NewWorkers(option)
	worker.Start()
	x := 0
	for i := 0; i < 10; i++ {
		ok := worker.Command(i, "a", fmt.Sprintf("%d", i))
		fmt.Println("cmd", i, ok)
		if ok {
			x++
		}
	}
	worker.Stop()
	worker.Sync()
	fmt.Println("ok", x)
}

func Fn(unit interface{}, meta map[string]string) {
	fmt.Println("do", unit, meta)
	time.Sleep(1 * time.Second)
}
