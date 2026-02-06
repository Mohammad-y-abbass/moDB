package storage

import (
	"encoding/binary"

	"github.com/mohammad-y-abbass/moDB/src/helpers"
)

const (
	LEAF_NODE     = 1
	INTERNAL_NODE = 2
)

const HEADER = 4
const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

type Node struct {
	data []byte
}

func init() {
	node1Max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	helpers.Assert(node1Max <= BTREE_PAGE_SIZE, "node exceeded page size")
}

/*
LittleEndian is used used to sort the bytes in data array
for example a pointer takes 8 bytes in the bytes array if we do it randomly it could be spread differently in different devices
this allows consistent encoding of bytes across different devices
*/

// header
func (node Node) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data)
}
func (node Node) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}
func (node Node) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// pointers
// this returns a slice from index pos to the end of the data array but since we are using LittleEndian.Uint64 it will return the first 8 bytes only
func (node Node) getPtr(idx uint16) uint64 {
	helpers.Assert(idx < node.nkeys(), "Out of bounds")
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.data[pos:])
}
func (node Node) setPtr(idx uint16, val uint64) {
	helpers.Assert(idx < node.nkeys(), "Out of bounds")
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node.data[pos:], val)
}

// offset list
// First offset is always 0 so the first offset stored in the list is the second offset
func offsetPos(node Node, idx uint16) uint16 {
	helpers.Assert(1 <= idx && idx <= node.nkeys(), "Out of bounds")
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}
func (node Node) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.data[offsetPos(node, idx):])
}
func (node Node) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node.data[offsetPos(node, idx):], offset)
}

// key-values
func (node Node) kvPos(idx uint16) uint16 {
	helpers.Assert(idx <= node.nkeys(), "Out of bounds")
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}
func (node Node) getKey(idx uint16) []byte {
	helpers.Assert(idx < node.nkeys(), "Out of bounds")
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	return node.data[pos+4:][:klen]
}
func (node Node) getVal(idx uint16) []byte {
	helpers.Assert(idx < node.nkeys(), "Out of bounds")
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos+0:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return node.data[pos+4+klen:][:vlen]
}

// node size in bytes
func (node Node) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

func nodeLookupLE(node Node, key []byte) uint16 {
	index, found := binarySearch(node, key)
	if found {
		return uint16(index)
	}

	if index > 0 {
		return uint16(index - 1)
	}
	return 0
}


