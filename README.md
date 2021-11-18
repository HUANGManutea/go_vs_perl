# go-vs-perl

# Prérequis
```
sudo apt install graphviz
```

# Time
TIMEFMT=$'real\t%E\nuser\t%U\nsys\t%S'

# Pprof

## PNG

terminal 1
```
cd go_web_server
go run .
```

terminal 2
```
go tool pprof -png http://localhost:6060/debug/pprof/heap > out.png
```

## HTTP

terminal 1
```
cd go_web_server
go run .
```

terminal 2
```
go tool pprof -http :8080 http://localhost:6060/debug/pprof/goroutine
```