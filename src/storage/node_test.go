package storage

import (
	"bytes"
	"testing"
)

func TestNodeHeaders(t *testing.T) {
	raw := make([]byte, BTREE_PAGE_SIZE)
	node := Node{data: raw}

	node.setHeader(LEAF_NODE, 5)
	if node.btype() != LEAF_NODE {
		t.Errorf("Expected btype %d, got %d", LEAF_NODE, node.btype())
	}
	if node.nkeys() != 5 {
		t.Errorf("Expected nkeys 5, got %d", node.nkeys())
	}
}

func TestNodePointers(t *testing.T) {
	raw := make([]byte, BTREE_PAGE_SIZE)
	node := Node{data: raw}
	node.setHeader(INTERNAL_NODE, 5) // nkeys=5 usually means 5 keys and 6 pointers for internal nodes?
	// Looking at getPtr: idx < nkeys. So pointers are 0 to nkeys-1?
	// Usually B-Trees have N keys and N+1 pointers.
	// Let's verify getPtr logic:
	// pos := HEADER + 8*idx.
	// If idx=0, pos=HEADER. If idx=nkeys-1, pos=HEADER+8*(nkeys-1).
	// So there are nkeys pointers? The code seems to imply 1 pointer per key?
	// Wait, getPtr check is idx < node.nkeys(). So 0..nkeys-1.

	val := uint64(123456789)
	node.setPtr(2, val)
	if got := node.getPtr(2); got != val {
		t.Errorf("Expected ptr %d, got %d", val, got)
	}
}

func TestNodeOffsets(t *testing.T) {
	raw := make([]byte, BTREE_PAGE_SIZE)
	node := Node{data: raw}
	node.setHeader(LEAF_NODE, 5)

	// Offset 0 is always 0 (implicit).
	// setOffset(idx, offset) writes to idx-1 position in the list?
	// check offsetPos: HEADER + 8*nkeys + 2*(idx-1).
	// Yes, idx must be >= 1.

	off := uint16(100)
	node.setOffset(1, off) // This is the first stored offset (index 1)
	if got := node.getOffset(1); got != off {
		t.Errorf("Expected offset %d, got %d", off, got)
	}

	// getOffset(0) returns 0
	if got := node.getOffset(0); got != 0 {
		t.Errorf("Expected offset 0 for index 0, got %d", got)
	}
}

func TestNodeKV(t *testing.T) {
	// To test getKey/getVal, we need to manually set up the node layout
	// Headers -> Pointers? (Only if internal? Code calculates offsets based on fields)
	// kvPos: HEADER + 8*nkeys + 2*nkeys + offset.
	// wait, 8*nkeys for pointers? Even for leaf nodes?
	// The code in `node.go` doesn't seem to differentiate between leaf and internal for `kvPos` calculation.
	// It assumes `HEADER + 8*node.nkeys() + ...`
	// This implies all nodes have space for pointers?

	raw := make([]byte, BTREE_PAGE_SIZE)
	node := Node{data: raw}
	node.setHeader(LEAF_NODE, 1)

	// Layout:
	// Header: 4 bytes
	// Pointers: 8 * 1 = 8 bytes
	// Offsets: 2 * 1 = 2 bytes. offsetPos for idx 1 is at HEADER + 8 + 0 = 12.
	// KV start: 4 + 8 + 2 = 14.

	// Set offset 1 to 0?
	// If offset 1 is 0, then key 1 starts at kvPos(1) -> returns 14 + 0 = 14.
	// But usually offset connects to idx.
	// getOffset(1) -> reads from offset list.

	// Let's create a key "key1" and value "val1".
	// Key "key1": len 4.
	// Value "val1": len 4.
	// Format in KV area: klen(2) vlen(2) key value
	// Total len: 2+2+4+4 = 12 bytes.

	// We want key 0 at offset 0?
	// node.getKey(0) -> kvPos(0) -> ... + getOffset(0) -> ... + 0. valid.

	// Let's write manually to the data buffer at the correct position.

	kvStart := uint16(4 + 8*1 + 2*1) // 14
	// set offset for idx 1 ?
	// getKey(0) uses offset 0 (0).
	// getKey(1) would use offset 1.
	// So for 1 key (idx 0), we don't need to set offset?
	// wait, nkeys=1 means idx 0 is valid.

	// write KV at kvStart
	pos := int(kvStart)
	// klen = 4
	raw[pos] = 4
	raw[pos+1] = 0
	// vlen = 4
	raw[pos+2] = 4
	raw[pos+3] = 0
	// key
	copy(raw[pos+4:], []byte("key1"))
	// val
	copy(raw[pos+8:], []byte("val1"))

	key := node.getKey(0)
	if !bytes.Equal(key, []byte("key1")) {
		t.Errorf("Expected key 'key1', got '%s'", string(key))
	}

	val := node.getVal(0)
	if !bytes.Equal(val, []byte("val1")) {
		t.Errorf("Expected val 'val1', got '%s'", string(val))
	}
}
