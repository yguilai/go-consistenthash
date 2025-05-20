package consistenthash

import (
	"sort"
	"strconv"
	"sync"
)

type (
	// HashFunc defines a hash method
	HashFunc func(data []byte) uint64

	// ConsistentHash is a hash ring implementation
	ConsistentHash[T Node] struct {
		hashFunc HashFunc
		replicas uint
		// circle sorted virtual nodes
		circle []uint64
		ring   map[uint64]T
		nodes  map[string]T
		mu     sync.RWMutex
	}

	Node interface {
		String() string
	}
)

const (
	minReplicas = uint(101)
	primeStr    = "16777619"
)

// New constructor for ConsistentHash
func New[T Node](opts ...Option) *ConsistentHash[T] {
	var ops options
	for _, opt := range opts {
		opt(&ops)
	}

	if ops.replicas < minReplicas {
		ops.replicas = minReplicas
	}
	if ops.hashFunc == nil {
		ops.hashFunc = defaultHashFunc
	}

	return &ConsistentHash[T]{
		hashFunc: ops.hashFunc,
		replicas: ops.replicas,
		ring:     make(map[uint64]T),
		nodes:    make(map[string]T),
	}
}

// NewWithNodes constructor with nodes for ConsistentHash
func NewWithNodes[T Node](nodes []T, opts ...Option) *ConsistentHash[T] {
	ch := New[T](opts...)
	ch.Add(nodes...)
	return ch
}

// Add adds some actual nodes, their replicas use h.replicas
func (h *ConsistentHash[T]) Add(nodes ...T) {
	h.AddReplicas(h.replicas, nodes...)
}

// AddReplicas adds some actual nodes with your replicas,
// if the replicas larger than h.replicas, it will be truncated.
func (h *ConsistentHash[T]) AddReplicas(replicas uint, nodes ...T) {
	if replicas > h.replicas {
		replicas = h.replicas
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, node := range nodes {
		nodeString := node.String()
		// only not exist node can be addition
		if h.contains(nodeString) {
			continue
		}
		h.nodes[nodeString] = node
		for i := uint(0); i < replicas; i++ {
			hash := h.hashFunc(h.getVnodeBytes(nodeString, i))
			h.circle = append(h.circle, hash)
			h.ring[hash] = node
		}
	}

	sort.Slice(h.circle, func(i, j int) bool {
		return h.circle[i] < h.circle[j]
	})
}

func (h *ConsistentHash[T]) contains(node string) bool {
	_, exists := h.nodes[node]
	return exists
}

// GetStringNodes get actual nodes from ConsistentHash with stringer
func (h *ConsistentHash[T]) GetStringNodes() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return mapKeys(h.nodes)
}

// GetNodes get actual nodes from ConsistentHash
func (h *ConsistentHash[T]) GetNodes() []T {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return mapValues(h.nodes)
}

// Remove removes the given nodes from ConsistentHash
func (h *ConsistentHash[T]) Remove(nodes ...T) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, node := range nodes {
		h.removeNode(node.String())
	}
}

// RemoveString removes the given node strings from ConsistentHash
func (h *ConsistentHash[T]) RemoveString(nodes ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, node := range nodes {
		h.removeNode(node)
	}
}

func (h *ConsistentHash[T]) removeNode(nodeString string) {
	if !h.contains(nodeString) {
		return
	}
	for i := uint(0); i < h.replicas; i++ {
		hash := h.hashFunc(h.getVnodeBytes(nodeString, i))
		index := sort.Search(len(h.circle), func(i int) bool {
			return h.circle[i] >= hash
		})
		if index < len(h.circle) && h.circle[index] == hash {
			h.circle = append(h.circle[:index], h.circle[index+1:]...)
		}
		delete(h.ring, hash)
	}
	delete(h.nodes, nodeString)
}

func (h *ConsistentHash[T]) getVnodeBytes(node string, i uint) []byte {
	return []byte(primeStr + node + strconv.FormatUint(uint64(i), 10))
}

// Get returns the corresponding actual node from ConsistentHash by the given key
func (h *ConsistentHash[T]) Get(key string) (T, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.get(key)
}

func (h *ConsistentHash[T]) get(key string) (node T, ok bool) {
	if len(h.ring) == 0 {
		return node, false
	}

	hash := h.hashFunc([]byte(key))
	index := sort.Search(len(h.circle), func(i int) bool {
		return h.circle[i] >= hash
	}) % len(h.circle)

	return h.ring[h.circle[index]], true
}

// GetString returns the corresponding actual node string from ConsistentHash by the given key
func (h *ConsistentHash[T]) GetString(key string) (string, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if node, ok := h.get(key); ok {
		return node.String(), true
	}
	return "", false
}
