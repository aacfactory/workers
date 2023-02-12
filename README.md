# WORKERS

workers pool for Golang

## Install

```go
go get -u github.com/aacfactory/workers
```

## Usage
Create worker task
```go
type Task struct {}

func (task *Task) Execute(ctx context.Context) {
    // todo: handle task
}

```
New workers and execute command
```go
ws := workers.New()

// dispatch task to worker
ctx := context.TODO()
if ok := ws.Dispatch(ctx, &Task{}); !ok {
    // todo: handle 'no workers remain' or 'closed' or 'context timeout'
}

// must dispatch task to worker 
ws.MustDispatch(ctx, &Task{})

ws.Close()
```

## Benchmark
Handler used in benchmark is sleeping 50 millisecond as handling command.
```text
goos: windows
goarch: amd64
pkg: github.com/aacfactory/workers
cpu: AMD Ryzen 9 3950X 16-Core Processor
BenchmarkNewWorkers
    worker_benchmark_test.go:37: total 1 accepted 1
    worker_benchmark_test.go:37: total 100 accepted 100
    worker_benchmark_test.go:37: total 10000 accepted 10000
    worker_benchmark_test.go:37: total 266420 accepted 266420
    worker_benchmark_test.go:37: total 834958 accepted 834958
    worker_benchmark_test.go:37: total 1104766 accepted 1104766
BenchmarkNewWorkers-32           1104766               953.7 ns/op            25
 B/op          1 allocs/op
PASS
```

## Thanks

* [valyala/fasthttp](https://github.com/valyala/fasthttp)

