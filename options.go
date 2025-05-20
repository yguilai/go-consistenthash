package consistenthash

type (
	options struct {
		hashFunc HashFunc
		replicas uint
	}

	Option func(*options)
)

// WithHashFunc defines a hash method for ConsistentHash
func WithHashFunc(hashFunc HashFunc) Option {
	return func(o *options) {
		o.hashFunc = hashFunc
	}
}

// WithReplicas defines a replicas number for ConsistentHash
func WithReplicas(replicas uint) Option {
	return func(o *options) {
		o.replicas = replicas
	}
}
