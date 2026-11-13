package httpapi

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"sync/atomic"

	"github.com/Rahul-Nikhate/quorumlog/internal/cluster"
	"github.com/Rahul-Nikhate/quorumlog/internal/raft"
)

// Demo is the public front of `qlogd --demo`: several LocalNodes in one
// process behind one port. It serves the dashboard at "/", the demo
// kill/restart endpoints, an aggregated /v1/cluster (one row per node and
// group), and proxies every other /v1/ and /metrics request to a live node,
// which forwards it to the group leader if needed.
type Demo struct {
	nodes     []*LocalNode
	events    *cluster.Events
	mux       *http.ServeMux
	proxies   map[raft.NodeID]*httputil.ReverseProxy
	next      atomic.Uint64
	dashboard http.Handler
}

// NewDemo builds the demo front for nodes (all sharing the events feed).
func NewDemo(nodes []*LocalNode, events *cluster.Events, dashboard http.Handler) *Demo {
	d := &Demo{nodes: nodes, events: events, mux: http.NewServeMux(), proxies: map[raft.NodeID]*httputil.ReverseProxy{}, dashboard: dashboard}
	for _, l := range nodes {
		target := &url.URL{Scheme: "http", Host: l.HTTPAddr()}
		id := l.ID()
		d.proxies[id] = &httputil.ReverseProxy{
			Rewrite: func(pr *httputil.ProxyRequest) { pr.SetURL(target) },
			ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
				if r.Context().Err() == nil {
					writeCode(w, http.StatusServiceUnavailable, CodeUnavailable, fmt.Sprintf("node %d unreachable: %v", id, err))
				}
			},
		}
	}
	d.mux.HandleFunc("POST /v1/demo/nodes/{id}/kill", d.kill)
	d.mux.HandleFunc("POST /v1/demo/nodes/{id}/restart", d.restart)
	d.mux.HandleFunc("GET /v1/cluster", d.clusterInfo)
	d.mux.HandleFunc("/v1/", d.proxy)
	d.mux.HandleFunc("/metrics", d.proxy)
	if dashboard != nil {
		d.mux.Handle("/", dashboard)
	}
	return d
}

func (d *Demo) ServeHTTP(w http.ResponseWriter, r *http.Request) { d.mux.ServeHTTP(w, r) }

// live returns a running node, round-robin (nil if all are down).
func (d *Demo) live() *LocalNode {
	start := d.next.Add(1)
	for i := range uint64(len(d.nodes)) {
		l := d.nodes[(start+i)%uint64(len(d.nodes))]
		if l.Node() != nil {
			return l
		}
	}
	return nil
}

func (d *Demo) proxy(w http.ResponseWriter, r *http.Request) {
	l := d.live()
	if l == nil {
		writeCode(w, http.StatusServiceUnavailable, CodeUnavailable, "all nodes are down; restart one")
		return
	}
	d.proxies[l.ID()].ServeHTTP(w, r)
}

func (d *Demo) find(w http.ResponseWriter, r *http.Request) *LocalNode {
	id, err := strconv.ParseUint(r.PathValue("id"), 10, 32)
	if err == nil {
		for _, l := range d.nodes {
			if uint64(l.ID()) == id {
				return l
			}
		}
	}
	writeCode(w, http.StatusNotFound, CodeNotFound, "no such node: "+r.PathValue("id"))
	return nil
}

func (d *Demo) kill(w http.ResponseWriter, r *http.Request) {
	l := d.find(w, r)
	if l == nil {
		return
	}
	if l.Node() != nil {
		d.events.Add(cluster.Event{Node: uint32(l.ID()), Msg: "killed (demo): node stopped, data dir kept"})
	}
	if err := l.Stop(); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (d *Demo) restart(w http.ResponseWriter, r *http.Request) {
	l := d.find(w, r)
	if l == nil {
		return
	}
	wasDown := l.Node() == nil
	if err := l.Start(); err != nil {
		writeError(w, errors.Join(errors.New("restart failed"), err))
		return
	}
	if wasDown {
		d.events.Add(cluster.Event{Node: uint32(l.ID()), Msg: "restarted (demo): recovered from its data dir"})
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// clusterInfo aggregates every live node's view (rows carry "node").
func (d *Demo) clusterInfo(w http.ResponseWriter, r *http.Request) {
	v := ClusterView{Demo: true, Nodes: []NodeView{}, Groups: []GroupView{}, Events: d.events.List()}
	for _, l := range d.nodes {
		n := l.Node()
		v.Nodes = append(v.Nodes, NodeView{ID: l.ID(), Addr: l.HTTPAddr(), Up: n != nil})
		if n == nil {
			continue
		}
		if v.NodeID == raft.None {
			v.NodeID = l.ID()
		}
		for _, g := range n.Groups() {
			v.Groups = append(v.Groups, GroupView{Node: l.ID(), GroupStatus: g})
		}
	}
	writeJSON(w, http.StatusOK, v)
}
