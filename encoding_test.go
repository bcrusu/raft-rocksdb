package raftrocksdb

import (
	"bytes"
	"math"
	"slices"
	"testing"
)

func TestEncodeLogKey(t *testing.T) {
	cases := []struct {
		id    uint32
		index uint64
	}{
		{0, 0},
		{0, math.MaxUint64},
		{math.MaxUint32, 0},
		{math.MaxUint32, math.MaxUint64},
	}

	for _, c := range cases {
		encoded := encodeLogKey(c.id, c.index)
		id, index := decodeLogKey(encoded)

		if id != c.id || index != c.index {
			t.Fail()
		}
	}
}

func TestEncodeLogKeySort(t *testing.T) {
	keys := [][]byte{
		encodeLogKey(1, math.MaxUint64),
		encodeLogKey(2, math.MaxUint64),
		encodeLogKey(1, 103),
		encodeLogKey(2, 103),
		encodeLogKey(1, 101),
		encodeLogKey(2, 101),
		encodeLogKey(1, 1<<35),
		encodeLogKey(2, 1<<35),
	}

	sorted := [][]byte{
		encodeLogKey(1, 101),
		encodeLogKey(1, 103),
		encodeLogKey(1, 1<<35),
		encodeLogKey(1, math.MaxUint64),
		encodeLogKey(2, 101),
		encodeLogKey(2, 103),
		encodeLogKey(2, 1<<35),
		encodeLogKey(2, math.MaxUint64),
	}

	slices.SortFunc(keys, bytes.Compare)

	for i, key := range keys {
		if !bytes.Equal(key, sorted[i]) {
			t.Fail()
		}
	}
}

func TestEncodeStableStoreKey(t *testing.T) {
	cases := []struct {
		id  uint32
		key []byte
	}{
		{0, []byte{}},
		{0, []byte("test_key")},
		{math.MaxUint32, []byte{}},
		{math.MaxUint32, []byte("test_key")},
	}

	for _, c := range cases {
		encoded := encodeStableKey(c.id, c.key)
		id, key := decodeStableKey(encoded)

		if id != c.id || !bytes.Equal(key, c.key) {
			t.Fail()
		}
	}
}
