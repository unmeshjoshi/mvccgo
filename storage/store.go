package storage

import (
	"bytes"
	"errors"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
	"math"
	"mvccgo/mvccpb"
)

var (
	keyBucketName  = []byte("key")
	metaBucketName = []byte("meta")

	consistentIndexKeyName  = []byte("consistent_index")
	scheduledCompactKeyName = []byte("scheduledCompactRev")
	finishedCompactKeyName  = []byte("finishedCompactRev")

	ErrCompacted = errors.New("mvcc: required revision has been compacted")
	ErrFutureRev = errors.New("mvcc: required revision is a future revision")
	ErrCanceled  = errors.New("mvcc: watcher is canceled")
)

const (
	// markedRevBytesLen is the byte length of marked revision.
	// The first `revBytesLen` bytes represents a normal revision. The last
	// one byte is the mark.
	markBytePosition       = markedRevBytesLen - 1
	markTombstone     byte = 't'
)

//Store exporting  new storage
type Store struct {
	lg *zap.Logger
	CurrentRev int64
	kvindex index
	db      *bolt.DB
}

func NewStore() *Store {
	l := zap.NewExample()
	db, err := bolt.Open("test.db", 0600, nil)
	if err != nil {
		l.Panic("failed to open database", zap.String("path", "test.db"), zap.Error(err))
	}

	tx, err := db.Begin(true)
	tx.CreateBucket(keyBucketName)
	tx.CreateBucket(metaBucketName)
	tx.Commit()

	return &Store{
		lg: l,
		CurrentRev: 0,
		kvindex: NewTreeIndex(l),
		db:      db,
	}
}

type RangeOptions struct {
	Limit int64
	Rev   int64
	Count bool
}

type RangeResult struct {
	KVs   []mvccpb.KeyValue
	Rev   int64
	Count int
}

func (s *Store) RangeKeys(key, end []byte, curRev int64, ro RangeOptions) (*RangeResult, error) {
	rev := ro.Rev
	if rev > curRev {
		return &RangeResult{KVs: nil, Count: -1, Rev: curRev}, ErrFutureRev
	}
	if rev <= 0 {
		rev = curRev
	}
	if ro.Count {
		total := s.kvindex.CountRevisions(key, end, rev, int(ro.Limit))
		s.lg.Info("count revisions from in-memory index tree")
		return &RangeResult{KVs: nil, Count: total, Rev: curRev}, nil
	}

	revpairs := s.kvindex.Revisions(key, end, rev, int(ro.Limit))
	if len(revpairs) == 0 {
		return &RangeResult{KVs: nil, Count: 0, Rev: curRev}, nil
	}

	limit := int(ro.Limit)
	if limit <= 0 || limit > len(revpairs) {
		limit = len(revpairs)
	}

	kvs := make([]mvccpb.KeyValue, limit)
	revBytes := newRevBytes()
	for i, revpair := range revpairs[:len(kvs)] {
		revToBytes(revpair, revBytes)
		_, vs := s.UnsafeRange(keyBucketName, revBytes, nil, 0)
		if len(vs) != 1 {
			s.lg.Fatal(
				"range failed to find revision pair",
				zap.Int64("revision-main", revpair.main),
				zap.Int64("revision-sub", revpair.sub),
			)
		}
		if err := kvs[i].Unmarshal(vs[0]); err != nil {
			s.lg.Fatal(
				"failed to unmarshal mvccpb.KeyValue",
				zap.Error(err),
			)
		}
	}
	//s.lg.Info("range keys from bolt db", zap.Int("lenth", len(kvs)))
	return &RangeResult{KVs: kvs, Count: len(revpairs), Rev: curRev}, nil
}
	//Put a given value into boltdb backend
func (s *Store) Put(key, value []byte) {
	rev := s.CurrentRev + 1
	c := rev
	_, created, ver, err := s.kvindex.Get(key, rev)
	if err == nil {
		c = created.main
	}
	ibytes := newRevBytes()

	changesLen := 1//len(tw.changes)

	idxRev := revision{main: rev, sub: int64(changesLen)}
	revToBytes(idxRev, ibytes)
	ver = ver + 1
	kv := mvccpb.KeyValue{
		Key:            key,
		Value:          value,
		CreateRevision: c,
		ModRevision:    rev,
		Version:        ver,
		Lease:          int64(1),
	}
	d, err := kv.Marshal()
	if err != nil {
		s.lg.Fatal(
			"failed to marshal mvccpb.KeyValue",
			zap.Error(err),
		)
	}
	s.kvindex.Put(key, idxRev)
	tx, err := s.db.Begin(true)
	bucket := tx.Bucket(keyBucketName)
	if bucket == nil {
		s.lg.Fatal(
			"failed to find a bucket",
			zap.String("bucket-name", string(keyBucketName)),
		)
	}
	seq := true
	if seq {
		// it is useful to increase fill percent when the workloads are mostly append-only.
		// this can delay the page split and reduce space usage.
		bucket.FillPercent = 0.9
	}
	if err := bucket.Put(ibytes, d); err != nil {
		s.lg.Fatal(
			"failed to write to a bucket",
			zap.String("bucket-name", string(keyBucketName)),
			zap.Error(err),
		)
	}
	defer tx.Commit()
	println("Put value ", s.CurrentRev)
	s.CurrentRev++

}


var safeRangeBucket = []byte("key")

func (s *Store) UnsafeRange(bucketName, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	if endKey == nil {
		// forbid duplicates for single keys
		limit = 1
	}
	if limit <= 0 {
		limit = math.MaxInt64
	}
	if limit > 1 && !bytes.Equal(bucketName, safeRangeBucket) {
		panic("do not use unsafeRange on non-keys bucket")
	}
	var keys, vals [][]byte = nil, nil
	tx, err := s.db.Begin(false)
	if (err != nil) {
		panic("Can not start transaction")
	}
	bucket := tx.Bucket(bucketName)
	c := bucket.Cursor()
	k2, v2 := unsafeRange(c, key, endKey, limit-int64(len(keys)))
	return append(k2, keys...), append(v2, vals...)
}

func unsafeRange(c *bolt.Cursor, key, endKey []byte, limit int64) (keys [][]byte, vs [][]byte) {
	if limit <= 0 {
		limit = math.MaxInt64
	}
	var isMatch func(b []byte) bool
	if len(endKey) > 0 {
		isMatch = func(b []byte) bool { return bytes.Compare(b, endKey) < 0 }
	} else {
		isMatch = func(b []byte) bool { return bytes.Equal(b, key) }
		limit = 1
	}

	for ck, cv := c.Seek(key); ck != nil && isMatch(ck); ck, cv = c.Next() {
		vs = append(vs, cv)
		keys = append(keys, ck)
		if limit == int64(len(keys)) {
			break
		}
	}
	return keys, vs
}