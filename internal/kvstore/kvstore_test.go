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
