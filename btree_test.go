package godb

import (
	"testing"
	"unsafe"
)

type C struct {
	tree     BTree
	expected map[string]string
	pages    map[uint64]BNode
}

func newC() *C {
	pages := map[uint64]BNode{}
	return &C{
		tree: BTree{
			get: func(ptr uint64) BNode {
				node, _ := pages[ptr]
				return node
			},
			new: func(node BNode) uint64 {
				key := uint64(uintptr(unsafe.Pointer(&node.data[0])))
				pages[key] = node
				return key
			},
			del: func(ptr uint64) {
				delete(pages, ptr)
			},
		},
		expected: map[string]string{},
		pages:    pages,
	}
}

func (c *C) get(key string) string {
	return string(c.tree.Get([]byte(key)))
}

func (c *C) add(key string, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.expected[key] = val
}

func (c *C) remove(key string) bool {
	delete(c.expected, key)
	return c.tree.Remove([]byte(key))
}

func TestBTree_Get(t *testing.T) {
	c := newC()
	c.add("key1", "val1")
	c.add("key2", "val2")
	c.add("key3", "val3")

	val := c.tree.Get([]byte("key2"))
	if c.get("key2") != "val2" {
		t.Errorf("expected to find key2 with value val2, but got %s", val)
	}

	if c.get("key4") != "" {
		t.Errorf("did not expect to find key4, but got %s", val)
	}
}

func TestBTree_Insert(t *testing.T) {
	c := newC()
	c.add("key1", "value1")
	c.add("key2", "value2")
	c.add("key3", "value3")
	c.add("key4", "value4")

	for key, val := range c.expected {
		if c.get(key) != val {
			t.Errorf("key %s: expected value %s, got different value %s", key, val, c.get(key))
		}
	}
}

func TestBTree_Remove(t *testing.T) {
	c := newC()
	c.add("key1", "value1")
	c.add("key2", "value2")
	c.add("key3", "value3")
	c.add("key4", "value4")

	c.remove("key2")

	if c.get("key2") != "" {
		t.Errorf("key2 was not removed properly")
	}
}

func TestBTree_InsertAndRemove(t *testing.T) {
	c := newC()
	c.add("key1", "value1")
	c.add("key2", "value2")
	c.add("key3", "value3")

	for key, val := range c.expected {
		if c.get(key) != val {
			t.Errorf("key %s: expected value %s, got different value", key, val)
		}
	}

	c.remove("key2")

	if c.get("key2") != "" {
		t.Errorf("key2 was not removed properly")
	}

	c.add("key4", "value4")

	for key, val := range c.expected {
		if c.get(key) != val {
			t.Errorf("key %s: expected value %s, got different value", key, val)
		}
	}
}
