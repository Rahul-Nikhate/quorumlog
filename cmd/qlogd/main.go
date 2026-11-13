// Command qlogd runs a QuorumLog node, or (--demo) a whole 3-node cluster in
// one process behind one HTTP port.
//
//	qlogd --id 1 --peers 1=h1:7001,2=h2:7001,3=h3:7001 \
//	      --http-peers 1=h1:8080,2=h2:8080,3=h3:8080 --http-addr :8080 --data-dir /data
//	qlogd --demo --data-dir /tmp/qlog        # dashboard + API on :$PORT (default 8080)
//
// Every flag can also be set by environment variable: QLOG_<FLAG> with dashes
// as underscores (QLOG_ID, QLOG_PEERS, QLOG_HTTP_PEERS, QLOG_HTTP_ADDR,
// QLOG_RAFT_ADDR, QLOG_DATA_DIR, QLOG_FSYNC, QLOG_DEMO). Flags win over env.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Rahul-Nikhate/quorumlog/internal/cluster"
	"github.com/Rahul-Nikhate/quorumlog/internal/dashboard"
	"github.com/Rahul-Nikhate/quorumlog/internal/httpapi"
	"github.com/Rahul-Nikhate/quorumlog/internal/log"
	"github.com/Rahul-Nikhate/quorumlog/internal/raft"
	"github.com/Rahul-Nikhate/quorumlog/internal/raft/transport"
)

type options struct {
	id        uint
	peers     string
	httpPeers string
	httpAddr  string
	raftAddr  string
	dataDir   string
	fsync     string
	demo      bool
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(os.Args[1:], logger); err != nil {
		fmt.Fprintln(os.Stderr, "qlogd:", err)
		os.Exit(1)
	}
}

func run(args []string, logger *slog.Logger) error {
	var o options
	fs := flag.NewFlagSet("qlogd", flag.ContinueOnError)
	fs.UintVar(&o.id, "id", 0, "this node's id (1..N)")
	fs.StringVar(&o.peers, "peers", "", `Raft addresses of all nodes: "1=host:port,2=host:port,3=host:port"`)
	fs.StringVar(&o.httpPeers, "http-peers", "", `HTTP addresses of all nodes (for forwarding to leaders): "1=host:port,..."`)
	fs.StringVar(&o.httpAddr, "http-addr", "", "HTTP listen address (default :8080; demo: :$PORT)")
	fs.StringVar(&o.raftAddr, "raft-addr", "", "Raft listen address (default: this node's --peers entry)")
	fs.StringVar(&o.dataDir, "data-dir", "./data", "data directory")
	fs.StringVar(&o.fsync, "fsync", "always", "fsync policy: always | interval=10ms | never")
	fs.BoolVar(&o.demo, "demo", false, "run a 3-node cluster in this process (dashboard demo)")
	if err := applyEnv(fs); err != nil {
		return err
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	sync, err := log.ParseSyncPolicy(o.fsync)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if o.demo {
		return runDemo(ctx, o, sync, logger)
	}
	return runNode(ctx, o, sync, logger)
}

// applyEnv sets flag defaults from QLOG_* environment variables.
func applyEnv(fs *flag.FlagSet) error {
	var err error
	fs.VisitAll(func(f *flag.Flag) {
		name := "QLOG_" + strings.ToUpper(strings.ReplaceAll(f.Name, "-", "_"))
		if v, ok := os.LookupEnv(name); ok && err == nil {
			if e := f.Value.Set(v); e != nil {
				err = fmt.Errorf("%s=%q: %w", name, v, e)
			}
		}
	})
	return err
}

// parsePeers parses "1=host:port,2=host:port".
func parsePeers(s string) (map[raft.NodeID]string, error) {
	out := map[raft.NodeID]string{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idStr, addr, ok := strings.Cut(part, "=")
		id, err := strconv.ParseUint(idStr, 10, 32)
		if !ok || err != nil || id == 0 || addr == "" {
			return nil, fmt.Errorf("bad peer %q (want id=host:port)", part)
		}
		if _, _, err := net.SplitHostPort(addr); err != nil {
			return nil, fmt.Errorf("bad peer address %q: %w", addr, err)
		}
		out[raft.NodeID(id)] = addr
	}
	return out, nil
}

func runNode(ctx context.Context, o options, sync log.SyncPolicy, logger *slog.Logger) error {
	id := raft.NodeID(o.id)
	peers, err := parsePeers(o.peers)
	if err != nil {
		return err
	}
	httpPeers, err := parsePeers(o.httpPeers)
	if err != nil {
		return err
	}
	if id == raft.None || peers[id] == "" {
		return fmt.Errorf("--id %d must be one of --peers %q", id, o.peers)
	}
	if o.httpAddr == "" {
		o.httpAddr = ":8080"
	}
	cc := cluster.Config{ID: id, Peers: peers, DataDir: o.dataDir, Sync: sync, Logger: logger.With("node", id)}
	if o.raftAddr != "" {
		cc.NewTransport = func(h transport.Handler) (transport.Transport, error) {
			return transport.NewTCP(transport.TCPConfig{ID: id, ListenAddr: o.raftAddr, Peers: peers, Handler: h, Logger: cc.Logger})
		}
	}
	node := httpapi.NewLocalNode(httpapi.LocalNodeConfig{Cluster: cc, HTTPAddr: o.httpAddr, HTTPPeers: httpPeers,
		Dashboard: dashboard.Handler(), Logger: cc.Logger})
	if err := node.Start(); err != nil {
		return err
	}
	logger.Info("qlogd node started", "id", id, "http", o.httpAddr, "raft", peers[id], "data", o.dataDir, "fsync", o.fsync)
	<-ctx.Done()
	logger.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return node.Shutdown(sctx)
}

func runDemo(ctx context.Context, o options, sync log.SyncPolicy, logger *slog.Logger) error {
	addr := o.httpAddr
	if addr == "" {
		port := os.Getenv("PORT")
		if port == "" {
			port = "8080"
		}
		addr = ":" + port
	}
	events := cluster.NewEvents(300)
	nodes, err := httpapi.NewLocalCluster(3, o.dataDir, cluster.Config{Sync: sync, Events: events, Logger: logger}, nil)
	if err != nil {
		return err
	}
	for _, n := range nodes {
		if err := n.Start(); err != nil {
			return err
		}
		logger.Info("demo node started", "id", n.ID(), "http", n.HTTPAddr())
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, n := range nodes {
			_ = n.Shutdown(sctx)
		}
	}()
	srv := httpapi.NewHTTPServer(httpapi.NewDemo(nodes, events, dashboard.Handler()))
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	logger.Info("qlogd demo: dashboard and API", "addr", ln.Addr().String(), "data", o.dataDir, "fsync", o.fsync)
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	logger.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		_ = srv.Close()
	}
	if err := <-errc; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
