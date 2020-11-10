package storage

import (
	"github.com/stretchr/testify/assert"
	"mvccgo/mvccpb"
	"testing"
)


func TestReadVersionRange(t *testing.T) {
	store := NewStore()
	//make 5 changes, new keys and updates to existing keys
	store.Put([]byte("key"), []byte("value"))
	store.Put([]byte("key"), []byte("value1"))
	store.Put([]byte("key1"), []byte("value2"))
	store.Put([]byte("key2"), []byte("value3"))
	store.Put([]byte("key2"), []byte("value4"))

	minBytes, maxBytes := newRevBytes(), newRevBytes()
	revToBytes(revision{main: 1}, minBytes)
	revToBytes(revision{main: store.CurrentRev + 1}, maxBytes)

	// UnsafeRange returns keys and values. And in boltdb, keys are revisions.
	// values are actual key-value pairs in backend.
	revs, vs := store.UnsafeRange(keyBucketName, minBytes, maxBytes, 0)

	var evs []mvccpb.Event
	evs = store.KvsToEvents(store.lg, revs, vs)

	assert.Equal(t, evs[0].Type, mvccpb.PUT)
	assert.Equal(t, evs[0].Kv.Value, []byte("value"))
	assert.Equal(t, evs[0].Kv.CreateRevision, int64(1))
	assert.Equal(t, evs[0].Kv.ModRevision, int64(1))

	assert.Equal(t, evs[1].Type, mvccpb.PUT)
	assert.Equal(t, evs[1].Kv.Value, []byte("value1"))
	assert.Equal(t, evs[1].Kv.CreateRevision, int64(1))
	assert.Equal(t, evs[1].Kv.ModRevision, int64(2))

	assert.Equal(t, evs[2].Type, mvccpb.PUT)
	assert.Equal(t, evs[2].Kv.Value, []byte("value2"))
	assert.Equal(t, evs[2].Kv.CreateRevision, int64(3))
	assert.Equal(t, evs[2].Kv.ModRevision, int64(3))

	assert.Equal(t, evs[3].Type, mvccpb.PUT)
	assert.Equal(t, evs[3].Kv.Value, []byte("value3"))
	assert.Equal(t, evs[3].Kv.CreateRevision, int64(4))
	assert.Equal(t, evs[3].Kv.ModRevision, int64(4))

	assert.Equal(t, evs[4].Type, mvccpb.PUT)
	assert.Equal(t, evs[4].Kv.Value, []byte("value4"))
	assert.Equal(t, evs[4].Kv.CreateRevision, int64(4))
	assert.Equal(t, evs[4].Kv.ModRevision, int64(5))
}