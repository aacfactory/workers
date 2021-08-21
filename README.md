# 概述

一个 Golang 的 goroutine 池。

## 安装

```go
go get -u github.com/aacfactory/workers
```

## 使用

```go
type Handler struct {
}

func (h *Handler) Handle(action string, payload interface{}) {
// todo
}

workers, workErr := workers.New(&Handler{})
if workErr != nil {
// handle error
return
}

workers.Start()

if ok := workers.Execute("test", i); !ok {
// handle 'not accepted'
}

workers.Stop()
```

### 性能对比

goos: windows <br>
goarch: amd64 <br>
pkg: github.com/aacfactory/workers <br>
cpu: AMD Ryzen 9 3950X 16-Core Processor <br>

### workers

BenchmarkNewWorkers-32: 923985, 1173 ns/op, 11 B/op, 1 allocs/op

### goroutine

BenchmarkChanWorkers-32: 64825, 16936 ns/op, 8 B/op, 0 allocs/op

## 参考感谢

* [valyala/fasthttp](https://github.com/valyala/fasthttp)

