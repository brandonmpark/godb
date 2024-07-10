package godb

import (
	"bytes"
	"encoding/binary"
)

func assert(condition bool, msg string) {
	if !condition {
		panic(msg)
	}
}

type BNode struct {
	data []byte
}

const (
	BNODE_NODE = 1
	BNODE_LEAF = 2
)

type BTree struct {
	root uint64

	get func(uint64) BNode
	new func(BNode) uint64
	del func(uint64)
}

// BNode: type | nKeys | pointers (nKeys) | offsets (nKeys) | KV pairs (nKeys)
// KV pair: kLen | vLen | key | val
const BTYPE_SIZE = 2
const N_KEYS_SIZE = 2
const BNODE_HEADER = BTYPE_SIZE + N_KEYS_SIZE
const POINTER_SIZE = 8
const OFFSET_SIZE = 2

const K_LEN_SIZE = 2
const V_LEN_SIZE = 2
const KV_HEADER = K_LEN_SIZE + V_LEN_SIZE

const PAGE_SIZE = 4096
const MAX_K_SIZE = 1000
const MAX_V_SIZE = 3000

// Gets the type of the node (either BNODE_NODE or BNODE_LEAF)
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data[:BTYPE_SIZE])
}

// Gets the number of keys in the node
func (node BNode) nKeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[BTYPE_SIZE:][:N_KEYS_SIZE])
}

// Sets the type and number of keys of the node
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[:BTYPE_SIZE], btype)
	binary.LittleEndian.PutUint16(node.data[BTYPE_SIZE:][:N_KEYS_SIZE], nkeys)
}

// Gets the i'th pointer in the node
func (node BNode) getPtr(i uint16) uint64 {
	assert(i < node.nKeys(), "i >= nKeys")
	pos := BNODE_HEADER + POINTER_SIZE*i
	return binary.LittleEndian.Uint64(node.data[pos:])
}

// Sets the i'th pointer in the node
func (node BNode) setPtr(i uint16, val uint64) {
	assert(i < node.nKeys(), "i >= nKeys")
	pos := BNODE_HEADER + POINTER_SIZE*i
	binary.LittleEndian.PutUint64(node.data[pos:], val)
}

// Calculates the position of the offset (relative to the start of the KV list) to the i'th KV pair in a node
func offsetPos(node BNode, i uint16) uint16 {
	assert(i >= 1 && i <= node.nKeys(), "i < 1 or i > nKeys")
	return BNODE_HEADER + POINTER_SIZE*node.nKeys() + OFFSET_SIZE*(i-1)
}

// Gets the i'th offset (relative to the start of the KV list) to the i'th KV pair in the node
func (node BNode) getOffset(i uint16) uint16 {
	assert(i >= 0 && i <= node.nKeys(), "i < 0 or i > nKeys")
	if i == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.data[offsetPos(node, i):][:OFFSET_SIZE])
}

// Sets the offset (relative to the start of the KV list) to the i'th KV pair in the node
func (node BNode) setOffset(i uint16, offset uint16) {
	assert(i >= 1 && i <= node.nKeys(), "i < 1 or i > nKeys")
	binary.LittleEndian.PutUint16(node.data[offsetPos(node, i):][:OFFSET_SIZE], offset)
}

// Calculates the position of the i'th KV pair in the node
func (node BNode) kvPos(i uint16) uint16 {
	assert(i <= node.nKeys(), "i > nKeys")
	return BNODE_HEADER + (POINTER_SIZE+OFFSET_SIZE)*node.nKeys() + node.getOffset(i)
}

// Gets the i'th key in the node
func (node BNode) getKey(i uint16) []byte {
	assert(i < node.nKeys(), "i >= nKeys")
	pos := node.kvPos(i)
	kLen := binary.LittleEndian.Uint16(node.data[pos:][:K_LEN_SIZE])
	return node.data[pos+KV_HEADER:][:kLen]
}

// Gets the i'th val in the node
func (node BNode) getVal(i uint16) []byte {
	assert(i < node.nKeys(), "i >= nKeys")
	pos := node.kvPos(i)
	kLen := binary.LittleEndian.Uint16(node.data[pos:][:K_LEN_SIZE])
	vLen := binary.LittleEndian.Uint16(node.data[pos+K_LEN_SIZE:][:V_LEN_SIZE])
	return node.data[pos+KV_HEADER+kLen:][:vLen]
}

// Gets the size of the node in bytes
func (node BNode) nBytes() uint16 {
	return node.kvPos(node.nKeys())
}

// Gets the index of the node that would contain a desired key
func nodeLookup(node BNode, key []byte) uint16 {
	for i := uint16(1); i < node.nKeys(); i++ {
		if cmp := bytes.Compare(node.getKey(i), key); cmp > 0 {
			return i - 1
		} else if cmp == 0 {
			return i
		}
	}

	return node.nKeys() - 1
}

// Gets the value for a key in the tree
func (tree *BTree) Get(key []byte) []byte {
	assert(len(key) != 0 && len(key) <= MAX_K_SIZE, "kLen = 0 or kLen > MAX_K_SIZE")
	if tree.root == 0 {
		return nil
	}

	curr := tree.get(tree.root)
	for {
		i := nodeLookup(curr, key)
		switch curr.btype() {
		case BNODE_LEAF:
			if bytes.Equal(curr.getKey(i), key) {
				return curr.getVal(i)
			}
			return nil
		case BNODE_NODE:
			curr = tree.get(curr.getPtr(i))
		default:
			panic("bad node")
		}
	}
}

// Appends n KV pairs copied from old (at index src) to new (at index dest)
func nodeAppendRange(new, old BNode, dest, src, n uint16) {
	assert(dest+n <= new.nKeys(), "dest + n > nKeys (new)")
	assert(src+n <= old.nKeys(), "src + n > nKeys (old)")
	if n == 0 {
		return
	}

	for i := uint16(0); i < n; i++ {
		new.setPtr(dest+i, old.getPtr(src+i))
	}

	destBegin := new.getOffset(dest)
	srcBegin := old.getOffset(src)
	for i := uint16(1); i <= n; i++ {
		offset := destBegin + old.getOffset(src+i) - srcBegin
		new.setOffset(dest+i, offset)
	}

	copy(new.data[new.kvPos(dest):], old.data[old.kvPos(src):old.kvPos(src+n)])
}

// Appends a single KV pair to a node at index i
func nodeAppend(node BNode, i uint16, ptr uint64, key []byte, val []byte) {
	assert(i <= node.nKeys(), "i > nKeys")

	node.setPtr(i, ptr)

	pos := node.kvPos(i)
	binary.LittleEndian.PutUint16(node.data[pos:][:K_LEN_SIZE], uint16(len(key)))
	binary.LittleEndian.PutUint16(node.data[pos+K_LEN_SIZE:][:V_LEN_SIZE], uint16(len(val)))
	copy(node.data[pos+KV_HEADER:], key)
	copy(node.data[pos+KV_HEADER+uint16(len(key)):], val)

	node.setOffset(i+1, node.getOffset(i)+KV_HEADER+uint16(len(key)+len(val)))
}

// Splits a node into two, left and right. The right is guaranteed to be smaller than the page size, and the node is guaranteed to only have at most one extra child over the limit.
func nodeSplit2(left BNode, right BNode, node BNode) {
	nKeys := node.nKeys()
	left.setHeader(node.btype(), nKeys-1)
	right.setHeader(node.btype(), 1)
	nodeAppendRange(left, node, 0, 0, nKeys-1)
	nodeAppendRange(right, node, 0, nKeys-1, 1)
	assert(right.nBytes() <= PAGE_SIZE, "nBytes (right split) > PAGE_SIZE")
}

// Splits a node into up to three. The node is guaranteed to only have at most one extra child over the limit.
func nodeSplit3(node BNode) (uint16, [3]BNode) {
	// 1 split
	if node.nBytes() <= PAGE_SIZE {
		node.data = node.data[:PAGE_SIZE]
		return 1, [3]BNode{node}
	}

	// 2 split
	left := BNode{make([]byte, 2*PAGE_SIZE)}
	right := BNode{make([]byte, PAGE_SIZE)}
	nodeSplit2(left, right, node)
	if left.nBytes() <= PAGE_SIZE {
		left.data = left.data[:PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}

	// 3 split
	leftLeft := BNode{make([]byte, PAGE_SIZE)}
	leftRight := BNode{make([]byte, PAGE_SIZE)}
	nodeSplit2(leftLeft, leftRight, left)
	assert(leftLeft.nBytes() <= PAGE_SIZE, "nBytes (leftmost split) > PAGE_SIZE")
	return 3, [3]BNode{leftLeft, leftRight, right}
}

// Replaces a child pointer with its splits
func nodeSplitChildPointers(tree *BTree, new, node BNode, i uint16, children ...BNode) {
	diff := uint16(len(children)) - 1
	new.setHeader(BNODE_NODE, node.nKeys()+diff)
	nodeAppendRange(new, node, 0, 0, i)
	for idx, node := range children {
		nodeAppend(new, i+uint16(idx), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, node, i+diff+1, i+1, node.nKeys()-(i+1))
}

// Inserts a KV pair into a leaf node at index i
func leafInsert(new, node BNode, i uint16, key, val []byte) {
	assert(i <= node.nKeys(), "i > nKeys")
	new.setHeader(BNODE_LEAF, node.nKeys()+1)
	nodeAppendRange(new, node, 0, 0, i)
	nodeAppend(new, i, 0, key, val)
	nodeAppendRange(new, node, i+1, i, node.nKeys()-i)
}

// Updates a KV pair in a leaf node at index i
func leafUpdate(new, node BNode, i uint16, key, val []byte) {
	assert(i < node.nKeys(), "i >= nKeys")
	new.setHeader(BNODE_LEAF, node.nKeys())
	nodeAppendRange(new, node, 0, 0, i)
	nodeAppend(new, i, 0, key, val)
	nodeAppendRange(new, node, i+1, i+1, node.nKeys()-(i+1))
}

// Inserts a KV pair into an internal node's i'th child
func nodeInsert(tree *BTree, node, new BNode, i uint16, key []byte, val []byte) {
	assert(i < node.nKeys(), "i >= nKeys")
	childPtr := node.getPtr(i)
	child := tree.get(childPtr)
	tree.del(childPtr)
	child = treeInsert(tree, child, key, val)
	nSplits, split := nodeSplit3(child)
	nodeSplitChildPointers(tree, new, node, i, split[:nSplits]...)
}

// Inserts a KV pair into a tree
func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	new := BNode{data: make([]byte, 2*PAGE_SIZE)}
	i := nodeLookup(node, key)

	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey(i)) {
			leafUpdate(new, node, i, key, val)
		} else {
			leafInsert(new, node, i+1, key, val)
		}
	case BNODE_NODE:
		nodeInsert(tree, new, node, i, key, val)
	default:
		panic("bad node")
	}
	return new
}

// Inserts a KV pair into the tree
func (tree *BTree) Insert(key []byte, val []byte) {
	assert(len(key) != 0 && len(key) <= MAX_K_SIZE, "kLen = 0 or kLen > MAX_K_SIZE")
	assert(len(val) <= MAX_V_SIZE, "vLen > MAX_V_SIZE")
	if tree.root == 0 {
		root := BNode{data: make([]byte, PAGE_SIZE)}
		root.setHeader(BNODE_LEAF, 2)
		nodeAppend(root, 0, 0, nil, nil) // Dummy key
		nodeAppend(root, 1, 0, key, val)
		tree.root = tree.new(root)
		return
	}

	node := tree.get(tree.root)
	tree.del(tree.root)
	node = treeInsert(tree, node, key, val)
	nSplits, split := nodeSplit3(node)
	if nSplits > 1 {
		root := BNode{data: make([]byte, PAGE_SIZE)}
		root.setHeader(BNODE_NODE, nSplits)
		for i, child := range split[:nSplits] {
			ptr, key := tree.new(child), child.getKey(0)
			nodeAppend(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(split[0])
	}
}

// Removes a key from a leaf node
func leafRemove(new, node BNode, i uint16) {
	assert(i < node.nKeys(), "i >= nKeys")
	new.setHeader(BNODE_LEAF, node.nKeys()-1)
	nodeAppendRange(new, node, 0, 0, i)
	nodeAppendRange(new, node, i, i+1, node.nKeys()-(i+1))
}

// Determines whether an updated child should merge with a sibling
func shouldMerge(tree *BTree, node BNode, i uint16, updated BNode) (int, BNode) {
	if updated.nBytes() > PAGE_SIZE/4 {
		return 0, BNode{}
	}
	if i > 0 {
		sibling := tree.get(node.getPtr(i - 1))
		if sibling.nBytes()+updated.nBytes()-BNODE_HEADER <= PAGE_SIZE {
			return -1, sibling
		}
	}
	if i < node.nKeys()-1 {
		sibling := tree.get(node.getPtr(i + 1))
		if sibling.nBytes()+updated.nBytes()-BNODE_HEADER <= PAGE_SIZE {
			return 1, sibling
		}
	}
	return 0, BNode{}
}

// Merges two nodes into one
func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(left.btype(), left.nKeys()+right.nKeys())
	nodeAppendRange(new, left, 0, 0, left.nKeys())
	nodeAppendRange(new, right, left.nKeys(), 0, right.nKeys())
}

// Replaces a single child pointer
func nodeReplaceChildPointer(new, node BNode, i uint16, ptr uint64, key []byte) {
	new.setHeader(BNODE_NODE, node.nKeys())
	nodeAppendRange(new, node, 0, 0, i)
	nodeAppend(new, i, ptr, key, nil)
}

// Removes a key from a node's i'th child
func nodeRemove(tree *BTree, node BNode, i uint16, key []byte) BNode {
	childPtr := node.getPtr(i)
	updatedChild := treeRemove(tree, tree.get(childPtr), key)
	if len(updatedChild.data) == 0 {
		return BNode{}
	}
	tree.del(childPtr)

	new := BNode{data: make([]byte, PAGE_SIZE)}
	mergeDirection, sibling := shouldMerge(tree, node, i, updatedChild)
	switch {
	case mergeDirection < 0:
		merged := BNode{data: make([]byte, PAGE_SIZE)}
		nodeMerge(merged, sibling, updatedChild)
		tree.del(node.getPtr(i - 1))
		nodeReplaceChildPointer(new, node, i-1, tree.new(merged), merged.getKey(0))
	case mergeDirection == 0:
		nodeSplitChildPointers(tree, new, node, i, updatedChild)
	case mergeDirection > 0:
		merged := BNode{data: make([]byte, PAGE_SIZE)}
		nodeMerge(merged, updatedChild, sibling)
		tree.del(node.getPtr(i + 1))
		nodeReplaceChildPointer(new, node, i, tree.new(merged), merged.getKey(0))
	}
	return new
}

// Removes a key from a tree
func treeRemove(tree *BTree, node BNode, key []byte) BNode {
	i := nodeLookup(node, key)
	switch node.btype() {
	case BNODE_LEAF:
		if !bytes.Equal(key, node.getKey(i)) {
			return BNode{}
		}
		new := BNode{data: make([]byte, PAGE_SIZE)}
		leafRemove(new, node, i)
		return new
	case BNODE_NODE:
		return nodeRemove(tree, node, i, key)
	default:
		panic("bad node")
	}
}

// Removes a key from the tree
func (tree *BTree) Remove(key []byte) bool {
	assert(len(key) != 0 && len(key) <= MAX_K_SIZE, "kLen = 0 or kLen > MAX_K_SIZE")
	if tree.root == 0 {
		return false
	}
	updated := treeRemove(tree, tree.get(tree.root), key)
	if len(updated.data) == 0 {
		return false
	}
	tree.del(tree.root)
	if updated.btype() == BNODE_NODE && updated.nKeys() == 1 {
		tree.root = updated.getPtr(0)
	} else {
		tree.root = tree.new(updated)
	}
	return true
}
