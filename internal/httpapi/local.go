package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"sync"

	"github.com/Rahul-Nikhate/quorumlog/internal/cluster"
	"github.com/Rahul-Nikhate/quorumlog/internal/raft"
)

// LocalNodeConfig configures a LocalNode.
type LocalNodeConfig struct {
	Cluster   cluster.Config
	HTTPAddr  string
	HTTPPeers map[raft.NodeID]string
	Dashboard http.Handler
	Logger    *slog.Logger
}

// LocalNode is one running node: a cluster.Node plus its HTTP API server.
// It can be stopped and started again from the same data directory and
// addresses (used by qlogd, demo kill/restart and the e2e test).
type LocalNode struct {
	cfg LocalNodeConfig

	mu     sync.Mutex
	node   *cluster.Node
	api    *Server
	srv    *http.Server
	served chan struct{}
}

// NewLocalNode returns a stopped node.
func NewLocalNode(cfg LocalNodeConfig) *LocalNode { return &LocalNode{cfg: cfg} }

// ID returns the node id.
func (l *LocalNode) ID() raft.NodeID { return l.cfg.Cluster.ID }

// HTTPAddr returns the HTTP address.
func (l *LocalNode) HTTPAddr() string { return l.cfg.HTTPAddr }

// Start opens the node (recovering from its data dir) and serves HTTP. It is
// a no-op if already running.
func (l *LocalNode) Start() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.node != nil {
		return nil
	}
	ln, err := net.Listen("tcp", l.cfg.HTTPAddr)
	if err != nil {
		return err
	}
	node, err := cluster.Open(l.cfg.Cluster)
	if err != nil {
		_ = ln.Close()
		return err
	}
	l.node = node
	l.api = New(Config{Node: node, HTTPPeers: l.cfg.HTTPPeers, Dashboard: l.cfg.Dashboard, Logger: l.cfg.Logger})
	l.srv = NewHTTPServer(l.api)
	l.served = make(chan struct{})
	go func(srv *http.Server, done chan struct{}) {
		defer close(done)
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) && l.cfg.Logger != nil {
			l.cfg.Logger.Error("httpapi: serve", "node", l.ID(), "err", err)
		}
	}(l.srv, l.served)
	return nil
}

// Stop kills the node abruptly: HTTP connections are closed (in-flight long
// polls end), then the Raft groups and the transport. The data dir is kept.
func (l *LocalNode) Stop() error { return l.stop(nil) }

// Shutdown is the graceful variant: in-flight requests may finish until ctx
// expires.
func (l *LocalNode) Shutdown(ctx context.Context) error { return l.stop(ctx) }

func (l *LocalNode) stop(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.node == nil {
		return nil
	}
	if ctx == nil || l.srv.Shutdown(ctx) != nil {
		_ = l.srv.Close()
	}
	<-l.served
	l.api.Close()
	err := l.node.Close()
	l.node, l.api, l.srv = nil, nil, nil
	return err
}

// Node returns the running cluster node (nil while stopped).
func (l *LocalNode) Node() *cluster.Node {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.node
}

// API returns the running HTTP API (nil while stopped).
func (l *LocalNode) API() *Server {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.api
}

// NewLocalCluster returns n stopped LocalNodes (ids 1..n) on 127.0.0.1 with
// free Raft and HTTP ports and data dirs dataDir/node-<id>. tmpl supplies the
// shared cluster settings (Sync, TickInterval, ElectionTick, Events, Logger).
// Used by demo mode and tests.
func NewLocalCluster(n int, dataDir string, tmpl cluster.Config, dashboard http.Handler) ([]*LocalNode, error) {
	raftAddrs := map[raft.NodeID]string{}
	httpAddrs := map[raft.NodeID]string{}
	for i := 1; i <= n; i++ {
		id := raft.NodeID(i)
		var err error
		if raftAddrs[id], err = freeAddr(); err != nil {
			return nil, err
		}
		if httpAddrs[id], err = freeAddr(); err != nil {
			return nil, err
		}
	}
	nodes := make([]*LocalNode, 0, n)
	for i := 1; i <= n; i++ {
		cc := tmpl
		cc.ID = raft.NodeID(i)
		cc.Peers = raftAddrs
		cc.DataDir = filepath.Join(dataDir, fmt.Sprintf("node-%d", i))
		nodes = append(nodes, NewLocalNode(LocalNodeConfig{Cluster: cc, HTTPAddr: httpAddrs[cc.ID], HTTPPeers: httpAddrs,
			Dashboard: dashboard, Logger: tmpl.Logger}))
	}
	return nodes, nil
}

// freeAddr returns a currently free 127.0.0.1 port (listen, then close).
func freeAddr() (string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer ln.Close()
	return ln.Addr().String(), nil
}
