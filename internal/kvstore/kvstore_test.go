package kvstore

import "testing"

func TestApplyAndGet(t *testing.T) {
	s := New()

	if _, ok := s.Get("missing"); ok {
		t.Fatal("empty store should not contain any key")
	}

	s.Apply("set", "a", "1")
	s.Apply("set", "b", "2")
	if v, ok := s.Get("a"); !ok || v != "1" {
		t.Fatalf("Get(a)=%q,%v; want 1,true", v, ok)
	}

	// Overwrite.
	s.Apply("set", "a", "99")
	if v, _ := s.Get("a"); v != "99" {
		t.Fatalf("after overwrite Get(a)=%q; want 99", v)
	}

	// Delete.
	s.Apply("delete", "b", "")
	if _, ok := s.Get("b"); ok {
		t.Fatal("Get(b) should be gone after delete")
	}

	// Unknown ops (e.g. a leader no-op marker) are ignored.
	s.Apply("noop", "", "")
	if snap := s.Snapshot(); len(snap) != 1 || snap["a"] != "99" {
		t.Fatalf("snapshot=%v; want {a:99}", snap)
	}
}

// TestDeterminism confirms the core replicated-state-machine property: applying
// the same command sequence to two independent stores yields identical state.
func TestDeterminism(t *testing.T) {
	type cmd struct{ op, k, v string }
	seq := []cmd{
		{"set", "x", "1"}, {"set", "y", "2"}, {"delete", "x", ""},
		{"set", "y", "3"}, {"set", "z", "9"}, {"delete", "q", ""},
	}
	a, b := New(), New()
	for _, c := range seq {
		a.Apply(c.op, c.k, c.v)
	}
	for _, c := range seq {
		b.Apply(c.op, c.k, c.v)
	}
	sa, sb := a.Snapshot(), b.Snapshot()
	if len(sa) != len(sb) {
		t.Fatalf("stores diverged: %v vs %v", sa, sb)
	}
	for k, v := range sa {
		if sb[k] != v {
			t.Fatalf("key %q: %q vs %q", k, v, sb[k])
		}
	}
}

// TestRestore confirms Restore replaces state wholesale (not a merge): keys
// absent from the given map must be gone afterward, not just overwritten ones.
func TestRestore(t *testing.T) {
	s := New()
	s.Apply("set", "stale", "x")
	s.Apply("set", "kept", "old")

	s.Restore(map[string]string{"kept": "new", "fresh": "1"})

	if v, ok := s.Get("stale"); ok {
		t.Fatalf("Get(stale)=%q,%v; want gone after Restore", v, ok)
	}
	if v, ok := s.Get("kept"); !ok || v != "new" {
		t.Fatalf("Get(kept)=%q,%v; want new,true", v, ok)
	}
	if v, ok := s.Get("fresh"); !ok || v != "1" {
		t.Fatalf("Get(fresh)=%q,%v; want 1,true", v, ok)
	}
}

// TestMarshalUnmarshalSnapshotRoundTrip confirms the JSON snapshot payload sent
// over InstallSnapshot round-trips exactly: what one store marshals, another
// must unmarshal into an identical state.
func TestMarshalUnmarshalSnapshotRoundTrip(t *testing.T) {
	src := New()
	src.Apply("set", "a", "1")
	src.Apply("set", "b", "2")
	src.Apply("delete", "a", "")
	src.Apply("set", "c", "3")

	data, err := src.MarshalSnapshot()
	if err != nil {
		t.Fatalf("MarshalSnapshot: %v", err)
	}

	dst := New()
	dst.Apply("set", "should-be-wiped", "yes") // must not survive UnmarshalSnapshot
	if err := dst.UnmarshalSnapshot(data); err != nil {
		t.Fatalf("UnmarshalSnapshot: %v", err)
	}

	want := src.Snapshot()
	got := dst.Snapshot()
	if len(got) != len(want) {
		t.Fatalf("restored snapshot=%v; want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("key %q: restored %q, want %q", k, got[k], v)
		}
	}
	if _, ok := got["should-be-wiped"]; ok {
		t.Fatal("UnmarshalSnapshot should replace state wholesale, not merge")
	}
}
