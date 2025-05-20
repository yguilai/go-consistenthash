package consistenthash

import (
	"crypto/sha256"
	"reflect"
	"strconv"
	"sync"
	"testing"
)

const (
	nodeSize   = 11
	smallCount = 10
	largeCount = 10000
)

type testNode string

func (n testNode) String() string { return string(n) }

func BenchmarkConsistentHash_Get(b *testing.B) {
	ch := New[testNode]()
	for i := 0; i < nodeSize; i++ {
		ch.Add(testNode("node:" + strconv.Itoa(i)))
	}

	for i := 0; i < b.N; i++ {
		ch.Get(strconv.Itoa(i))
	}
}

func TestNew(t *testing.T) {
	ch := New[testNode]()
	if ch.replicas != minReplicas {
		t.Fatalf("expected default replicas %d, got %d", minReplicas, ch.replicas)
	}

	ch = New[testNode](WithHashFunc(func(data []byte) uint64 {
		h := sha256.Sum256(data)
		return uint64(h[0])<<56 | uint64(h[1])<<48 | uint64(h[2])<<40 | uint64(h[3])<<32 |
			uint64(h[4])<<24 | uint64(h[5])<<16 | uint64(h[6])<<8 | uint64(h[7])
	}), WithReplicas(200))
	if ch.replicas != 200 {
		t.Fatalf("expected custom replicas 200, got %d", ch.replicas)
	}
}

func TestNewWithNodes(t *testing.T) {
	node1 := testNode("localhost1")
	ch := NewWithNodes[testNode]([]testNode{node1})

	for i := 0; i < smallCount; i++ {
		val, ok := ch.GetString(strconv.Itoa(i))
		if val != node1.String() || !ok {
			t.Fatalf("expected %s, got %s, ok %v", node1.String(), val, ok)
		}
	}
}

func TestConsistentHash_AddReplicas(t *testing.T) {
	ch := New[testNode]()
	nodes := []testNode{"node1"}

	ch.AddReplicas(5, nodes...)
	if len(ch.circle) != 5 {
		t.Fatalf("expected 5 virtual nodes, got %d", len(ch.circle))
	}
}

func TestConsistentHash_GetStringNodes(t *testing.T) {
	ch := New[testNode]()
	nodes := []testNode{"node1", "node2"}
	stringNodes := make([]string, len(nodes))
	for i, node := range nodes {
		stringNodes[i] = node.String()
	}

	ch.Add(nodes...)

	chStringNodes := ch.GetStringNodes()
	if !reflect.DeepEqual(chStringNodes, stringNodes) {
		t.Fatalf("expected %v, got %v", stringNodes, chStringNodes)
	}
}

func TestConsistentHash_GetNodes(t *testing.T) {
	ch := New[testNode]()
	nodes := []testNode{"node1", "node2"}

	ch.Add(nodes...)

	chNodes := ch.GetNodes()
	if !reflect.DeepEqual(chNodes, nodes) {
		t.Fatalf("expected %v, got %v", nodes, chNodes)
	}
}

func TestConsistentHash_RemoveAndString(t *testing.T) {
	ch := New[testNode]()
	node1 := testNode("node1")
	node2 := testNode("node2")

	ch.Add(node1, node2)
	ch.Remove(node1)

	for i := 0; i < smallCount; i++ {
		n, _ := ch.Get(strconv.Itoa(i))
		if n != node2 {
			t.Fatalf("expected %s, got %s", node2, n)
		}

		nn, _ := ch.GetString(strconv.Itoa(i))
		if nn != node2.String() {
			t.Fatalf("expected %s, got %s", node2, nn)
		}
	}
}

func TestConcurrentAccess(t *testing.T) {
	ch := New[testNode]()
	nodes := []testNode{"node1", "node2", "node3"}
	ch.Add(nodes...)

	var wg sync.WaitGroup
	for i := 0; i < largeCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "key" + strconv.Itoa(i)
			_, ok := ch.Get(key)
			if !ok {
				t.Error("failed to get node for key:", key)
			}
		}(i)
	}
	wg.Wait()
}

func TestHashDistribution(t *testing.T) {
	ch := New[testNode]()
	for i := 0; i < nodeSize; i++ {
		ch.Add(testNode("node" + strconv.Itoa(i)))
	}

	keyMappings := make(map[int]string, largeCount)
	for i := 0; i < largeCount; i++ {
		key := strconv.Itoa(largeCount + i)
		node, ok := ch.Get(key)
		if !ok {
			t.Error("failed to get node for key:", key)
		}
		keyMappings[i] = node.String()
	}

	removeNode := "node7"
	ch.RemoveString(removeNode)
	newKeyMapping := make(map[int]string, largeCount)
	for i := 0; i < largeCount; i++ {
		key := strconv.Itoa(largeCount + i)
		node, ok := ch.Get(key)
		if !ok {
			t.Error("failed to get node for key:", key)
		}
		if removeNode == node.String() {
			t.Fatal("expected removed node to be removed")
		}
		newKeyMapping[i] = node.String()
	}

	var remappingCount int
	for k, v := range newKeyMapping {
		if v != keyMappings[k] {
			remappingCount++
		}
	}

	ratio := float32(remappingCount) / float32(largeCount)
	expectedMaxRatio := 2.5 / float32(nodeSize)
	if ratio > expectedMaxRatio {
		t.Fatalf("unbalanced distribution, max ratio %f, got ratio %f", expectedMaxRatio, ratio)
	}
}
