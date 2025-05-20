# go-consistenthash

a consistent hash implementation that inspired by [go-zero](https://github.com/zeromicro/go-zero)

this library is without third dependency, the go sdk should be since 1.18, because of this library use generic feature

## Usage

```bash
go get github.com/yguilai/go-consistenthash
```

### Simple Use Case

```go
package main

import (
	"fmt"
	consistenthash "github.com/yguilai/go-consistenthash"
)

type StringNode string

// implement consistenthash.Node interface
func (s StringNode) String() string {
	return string(s)
}

func main() {
	// use default options
	ch := consistenthash.New[StringNode]()
	nodes := []StringNode{"localhost:1", "localhost:2", "localhost:3"}
	ch.Add(nodes...)

	node, ok := ch.Get("key1")
	fmt.Printf("node: %v, ok: %v", node, ok)
}
```
