package main

import (
	"encoding/gob"
	"encoding/json"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

const (
	opSet kvOp = iota
	opDelete
)

type kvOp uint8

type storeEvent struct {
	Operation kvOp   `json:"o"`
	Key       string `json:"k"`
	Value     string `json:"v"`
}

func (e storeEvent) ToBytes() []byte {
	b, _ := json.Marshal(e)
	return b
}

func newKV() *keyValueStore {
	return &keyValueStore{
		kv: make(map[string]string),
	}
}

type keyValueStore struct {
	m  sync.RWMutex
	kv map[string]string
}

func (k *keyValueStore) Get(key string) (string, bool) {
	k.m.RLock()
	defer k.m.RUnlock()
	value, found := k.kv[key]
	return value, found
}

func (k *keyValueStore) Apply(l *raft.Log) any {
	k.m.Lock()
	defer k.m.Unlock()

	var evt storeEvent
	json.Unmarshal(l.Data, &evt)
	switch evt.Operation {
	case opSet:
		k.kv[evt.Key] = evt.Value
	case opDelete:
		delete(k.kv, evt.Key)
	}
	return nil
}

func (k *keyValueStore) Snapshot() (raft.FSMSnapshot, error) {
	return k, nil
}

func (k *keyValueStore) Restore(r io.ReadCloser) error {
	k.m.Lock()
	defer k.m.Unlock()
	d := gob.NewDecoder(r)
	return d.Decode(&k.kv)
}

func (k *keyValueStore) Persist(sink raft.SnapshotSink) error {
	k.m.Lock()
	defer k.m.Unlock()
	e := gob.NewEncoder(sink)
	return e.Encode(k.kv)
}

func (k *keyValueStore) Release() {
}
