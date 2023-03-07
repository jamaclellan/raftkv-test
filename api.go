package main

import (
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/hashicorp/raft"
)

type kvApi struct {
	http.Handler
	kv   *keyValueStore
	raft *raft.Raft
}

func (a *kvApi) getValue(w http.ResponseWriter, r *http.Request) {
	value, found := a.kv.Get(chi.URLParam(r, "key"))
	if !found {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(value))
}

func (a *kvApi) setValue(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	body, _ := io.ReadAll(r.Body)
	defer r.Body.Close()
	f := a.raft.Apply(storeEvent{
		Operation: opSet,
		Key:       key,
		Value:     string(body),
	}.ToBytes(), time.Second)
	if err := f.Error(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("%d", f.Index())))
}

func (a *kvApi) deleteValue(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	f := a.raft.Apply(storeEvent{
		Operation: opDelete,
		Key:       key,
	}.ToBytes(), time.Second)
	if err := f.Error(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("%d", f.Index())))
}

func (a *kvApi) raftAdd(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	body, _ := io.ReadAll(r.Body)
	defer r.Body.Close()

	f := a.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(body), 0, 30*time.Second)
	if err := f.Error(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("%d", f.Index())))
}

func (a *kvApi) raftRemove(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	f := a.raft.RemoveServer(raft.ServerID(id), 0, 30*time.Second)
	if err := f.Error(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("%d", f.Index())))
}

func (a *kvApi) raftSnapshot(w http.ResponseWriter, r *http.Request) {
	f := a.raft.Snapshot()
	if err := f.Error(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
}

func newHTTPKV(kv *keyValueStore, r *raft.Raft) *kvApi {
	api := &kvApi{
		kv:   kv,
		raft: r,
	}

	router := chi.NewRouter()
	router.Use(middleware.Logger)

	router.Get("/{key}", api.getValue)
	router.Put("/{key}", api.setValue)
	router.Delete("/{key}", api.deleteValue)

	router.Put("/_raft/{id}", api.raftAdd)
	router.Delete("/_raft/{id}", api.raftRemove)
	router.Get("/_raft/snapshot", api.raftSnapshot)

	api.Handler = router
	return api
}
