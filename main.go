package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	bolt_store "github.com/hashicorp/raft-boltdb/v2"
)

var (
	rpcAddr  = flag.String("address", "[::1]:3631", "host + port for this node.")
	raftAddr = flag.String("raft_address", "[::1]:3632", "host + post for this node raft.")
	raftId   = flag.String("raft_id", "", "Node ID used during raft operations.")

	raftStorageDir = flag.String("raft_storage_dir", "raft_data/", "Raft storage directory.")
	raftBootstrap  = flag.Bool("raft_bootstrap", false, "Run Raft in bootstrap mode.")
)

func main() {
	flag.Parse()

	if *raftId == "" {
		log.Fatal("must specify a raft node id using --raft_id")
	}

	kv := newKV()
	r, err := newRaft(*raftId, *raftAddr, kv)
	if err != nil {
		log.Fatalf("failed to start raft process: %v", err)
	}

	api := newHTTPKV(kv, r)

	if err := http.ListenAndServe(*rpcAddr, api); err != nil {
		log.Fatalf("failed to run gRPC: %v", err)
	}
}

func newRaft(id, addr string, fsm raft.FSM) (*raft.Raft, error) {
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(id)

	baseDir := filepath.Join(*raftStorageDir, id)
	if err := os.MkdirAll(baseDir, 0666); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("newRaft.mkdir(%q): %w", baseDir, err)
	}

	logPath := filepath.Join(baseDir, "logs")
	logDb, err := bolt_store.NewBoltStore(logPath)
	if err != nil {
		return nil, fmt.Errorf("newRaft.NewLogs(%q): %w", logPath, err)
	}

	stablePath := filepath.Join(baseDir, "stable")
	stableDb, err := bolt_store.NewBoltStore(stablePath)
	if err != nil {
		logDb.Close()
		return nil, fmt.Errorf("newRaft.NewStable(%q): %w", stablePath, err)
	}

	snapshotter, err := raft.NewFileSnapshotStore(baseDir, 3, os.Stderr)

	netAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		logDb.Close()
		stableDb.Close()
		return nil, fmt.Errorf("newRaft.ResolveIPAddr(%q): %w", addr, err)
	}
	tran, err := raft.NewTCPTransport(addr, netAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		logDb.Close()
		stableDb.Close()
		return nil, fmt.Errorf("newRaft.NewTransport(%q, %q): %w", addr, addr, err)
	}

	if *raftBootstrap {
		config := raft.Configuration{
			Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(id),
					Address:  raft.ServerAddress(addr),
				},
			},
		}
		if err := raft.BootstrapCluster(cfg, logDb, stableDb, snapshotter, tran, config); err != nil {
			return nil, fmt.Errorf("newRaft.Bootstrap: %w", err)
		}
	}

	r, err := raft.NewRaft(cfg, fsm, logDb, stableDb, snapshotter, tran)
	if err != nil {
		logDb.Close()
		stableDb.Close()
		return nil, fmt.Errorf("newRaft.NewRaft: %w", err)
	}

	return r, nil
}
