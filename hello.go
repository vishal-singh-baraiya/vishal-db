package main

import (
	"encoding/binary" // Import this for binary encoding and decoding
	//"github.com/stretchr/testify/assert"
)

const HEADER = 4
const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

const (
	BNODE_NODE = 1
	BNODE_LEAF = 2
)

func main() {
	// Create a new B-Tree
}

// header
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data)
}
func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// pointers
func (node BNode) getPtr(idx uint16) uint64 {
	if idx > node.nkeys() {
		panic("Error in Keys")
	}
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.data[pos:])
}
func (node BNode) setPtr(idx uint16, val uint64) {
	if idx > node.nkeys() {
		panic("Error in Keys")
	}
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node.data[pos:], val)
}
func init() {
	node1max := (HEADER + BTREE_MAX_KEY_SIZE + 14 + BTREE_MAX_VAL_SIZE)

	if node1max > BTREE_PAGE_SIZE {
		panic("Error in  B-Tree page size")
	}

}

type BNode struct {
	data []byte
}

type BTree struct {
	root uint64
	get  func(uint64) BNode
	new  func(BNode) uint64
	del  func(uint64)
}
