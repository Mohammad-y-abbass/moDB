package storage

import (
	"testing"
)

func TestBTreeInit(t *testing.T) {
	bt := BTree{root: 0}
	if bt.root != 0 {
		t.Errorf("Expected root 0, got %d", bt.root)
	}
}
