package storage

import (
	"testing"
)

// Helper to setup a node with sorted keys for testing
func setupNodeWithKeys(keys []string) Node {
	n := len(keys)
	raw := make([]byte, BTREE_PAGE_SIZE)
	node := Node{data: raw}
	node.setHeader(LEAF_NODE, uint16(n))

	// Calculate start of KV area
	// kvPos base: HEADER + 8*n + 2*n
	kvBase := uint16(4 + 8*n + 2*n)

	currentOffset := uint16(0)
	for i, k := range keys {
		// Set offset for next key (if not last)??
		// offsets are 1-based indices in the list?
		// Logic: getOffset(i) returns offset.
		// We need to setOffset(i+1, nextOffset) ?
		// Usually, offset[i] points to where key[i] starts relative to kvBase.
		// getOffset(0) is 0.
		// getOffset(1) should be length of KV pair 0.

		// Write Key at kvBase + currentOffset
		pos := int(kvBase + currentOffset)
		kBytes := []byte(k)
		vBytes := []byte("val") // dummy value

		klen := uint16(len(kBytes))
		vlen := uint16(len(vBytes))

		// write klen
		raw[pos] = byte(klen)
		raw[pos+1] = byte(klen >> 8)
		// write vlen
		raw[pos+2] = byte(vlen)
		raw[pos+3] = byte(vlen >> 8)
		// write key
		copy(raw[pos+4:], kBytes)
		// write val
		copy(raw[pos+4+int(klen):], vBytes)

		// update offset for next One
		entryLen := 2 + 2 + klen + vlen
		currentOffset += entryLen

		// If there is a next key, we need to record its offset?
		// Actually, we record offset for i+1.
		if i < n-1 {
			node.setOffset(uint16(i+1), currentOffset)
		}
	}
	return node
}

func TestBinarySearch(t *testing.T) {
	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	node := setupNodeWithKeys(keys)

	// Test exact matches
	for i, k := range keys {
		idx, found := binarySearch(node, []byte(k))
		if !found {
			t.Errorf("Expected to find key %s", k)
		}
		if idx != i {
			t.Errorf("Expected index %d for key %s, got %d", i, k, idx)
		}
	}

	// Test non-existent keys (insertion points)
	tests := []struct {
		target    string
		expectIdx int
	}{
		{"aardvark", 0}, // before apple
		{"carrot", 2},   // between banana and cherry
		{"fig", 5},      // after elderberry
	}

	for _, tc := range tests {
		idx, found := binarySearch(node, []byte(tc.target))
		if found {
			t.Errorf("Expected not to find key %s", tc.target)
		}
		if idx != tc.expectIdx {
			t.Errorf("Expected insertion index %d for key %s, got %d", tc.expectIdx, tc.target, idx)
		}
	}
}
