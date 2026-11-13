package main

import "testing"

// The RST rule goes to the raw table, ahead of conntrack: in filter the
// tracker still sees the kernel's RST, calls the connection closed, and a
// firewall that drops INVALID packets (ufw does) throws every reply away —
// the client connects, nothing loads.
func TestL3RuleInRawTable(t *testing.T) {
	ipt := &fakeIPT{rules: map[string]bool{}}
	a := newTestAgent(t, "run", t.TempDir(), ipt)
	s := testSpec()
	if _, err := a.apply(s); err != nil {
		t.Fatal(err)
	}
	if !ipt.in("raw") || ipt.in("filter") {
		t.Fatalf("rule: raw %v, filter %v; want raw only", ipt.in("raw"), ipt.in("filter"))
	}
	if err := a.remove(s.Channel); err != nil {
		t.Fatal(err)
	}
	if ipt.has() {
		t.Fatal("the rule stayed after the last l3 channel went")
	}
}

// An older agent (or node-install's l3-rst.sh) left the rule in filter: it
// moves to raw.
func TestL3RuleMovesOutOfFilter(t *testing.T) {
	ipt := &fakeIPT{rules: map[string]bool{"filter " + rstRule: true}}
	a := newTestAgent(t, "run", t.TempDir(), ipt)
	if _, err := a.apply(testSpec()); err != nil {
		t.Fatal(err)
	}
	if !ipt.in("raw") || ipt.in("filter") {
		t.Fatalf("rule: raw %v, filter %v; want it moved to raw", ipt.in("raw"), ipt.in("filter"))
	}
}

// A kernel without the raw table keeps the rule in filter, as before, and an
// l3 channel still starts.
func TestL3RuleFallsBackToFilter(t *testing.T) {
	ipt := &fakeIPT{rules: map[string]bool{}, noRaw: true}
	a := newTestAgent(t, "run", t.TempDir(), ipt)
	s := testSpec()
	if _, err := a.apply(s); err != nil {
		t.Fatalf("l3 without the raw table: %v", err)
	}
	if !ipt.in("filter") {
		t.Fatal("no rule in filter")
	}
	a.mu.Lock()
	err := a.syncL3()
	a.mu.Unlock()
	if err != nil || !ipt.in("filter") {
		t.Fatalf("a second sync: %v, rule %v", err, ipt.in("filter"))
	}
	if err := a.remove(s.Channel); err != nil {
		t.Fatal(err)
	}
	if ipt.has() {
		t.Fatal("the rule stayed after the last l3 channel went")
	}
}
