package raft

import "testing"

func TestInflights(t *testing.T) {
	in := newInflights(3, 250)
	in.add(1, 100)
	in.add(2, 100)
	if in.full() {
		t.Fatal("full too early")
	}
	in.add(5, 100)
	if !in.full() {
		t.Fatal("not full at 3 msgs")
	}
	in.freeLE(2)
	if in.count != 1 || in.bytes != 100 || in.full() {
		t.Fatalf("after freeLE(2): count=%d bytes=%d", in.count, in.bytes)
	}
	in.add(7, 200)
	if !in.full() {
		t.Fatal("byte limit not enforced")
	}
	in.freeFirst()
	if in.count != 1 || in.buf[in.start].index != 7 {
		t.Fatalf("freeFirst: count=%d", in.count)
	}
	// Wrap-around.
	in.add(8, 1)
	in.add(9, 1)
	in.freeLE(8)
	if in.count != 1 || in.buf[in.start].index != 9 {
		t.Fatal("wrap-around")
	}
}

func TestProgressTransitions(t *testing.T) {
	pr := newProgress(11, 4, 0)
	if pr.isPaused() {
		t.Fatal("fresh probe paused")
	}
	pr.sentEntries(12, 10, true)
	if !pr.isPaused() || pr.Next != 11 {
		t.Fatalf("probe after send: paused=%v next=%d", pr.isPaused(), pr.Next)
	}
	if !pr.maybeUpdate(12) || pr.maybeUpdate(12) || pr.Next != 13 {
		t.Fatalf("maybeUpdate: match=%d next=%d", pr.Match, pr.Next)
	}
	pr.becomeReplicate()
	for i := uint64(0); i < 4; i++ {
		pr.sentEntries(13+i, 1, true)
	}
	if !pr.isPaused() || pr.Next != 17 {
		t.Fatalf("replicate window: paused=%v next=%d", pr.isPaused(), pr.Next)
	}
	pr.becomeSnapshot(30)
	if !pr.isPaused() {
		t.Fatal("snapshot state not paused")
	}
	pr.becomeProbe()
	if pr.Next != 31 || pr.State != StateProbe {
		t.Fatalf("after snapshot → probe next=%d", pr.Next)
	}
}
