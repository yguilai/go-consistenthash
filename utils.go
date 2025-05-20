package consistenthash

import "hash/crc32"

func mapKeys[Map ~map[K]V, K comparable, V any](m Map) []K {
	result := make([]K, 0, len(m))
	for k := range m {
		result = append(result, k)
	}
	return result
}

func mapValues[Map ~map[K]V, K comparable, V any](m Map) []V {
	result := make([]V, 0, len(m))
	for _, v := range m {
		result = append(result, v)
	}
	return result
}

// defaultHashFunc a crc32 hash method, but returns uint64
func defaultHashFunc(data []byte) uint64 {
	return uint64(crc32.ChecksumIEEE(data))
}
