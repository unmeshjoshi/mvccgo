package main

import (
	s "mvccgo/storage"
)

func main() {
	store := s.NewStore()
	store.Put([]byte("key"), []byte("value"))
	store.Put([]byte("key"), []byte("value1"))
	store.Put([]byte("key"), []byte("value2"))
	store.Put([]byte("key"), []byte("value3"))
	for i := 1; i < 5; i++ {
		opt := s.RangeOptions{Rev: int64(i)}
		result, err := store.RangeKeys([]byte("key"), nil, store.CurrentRev, opt)
		if (err != nil) {
			panic(err)
		}
		println(string(result.KVs[0].Key), string(result.KVs[0].Value))
		println()
	}
}
