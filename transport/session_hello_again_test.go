package transport

import (
	"testing"
	"time"
)

// The exit's answer to a hello can be lost: on a striped carrier whose lanes
// are still coming up it goes to a room the client does not read yet (live,
// Avito). The client keeps sending the zero echo; the exit must answer it
// again, or the handshake never completes.
func TestExitAnswersAgainWhenItsAnswerWasLost(t *testing.T) {
	client, exit, _, exitWire := sessionPair(t)
	client.handshakeTimeout = 8 * time.Second
	exitWire.mu.Lock()
	exitWire.drop = true
	exitWire.mu.Unlock()
	if err := exit.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- client.Start() }()
	eventually(t, "the exit hears the client", func() bool { return exit.cntHelloRecv.Load() >= 3 })
	exitWire.mu.Lock()
	exitWire.drop = false
	exitWire.mu.Unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("handshake after the lost answer: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no handshake")
	}
}

// The answers again are paced: a client that keeps calling while its lanes
// come up does not get one answer per hello.
func TestExitAnswersAgainAtMostOnceASecond(t *testing.T) {
	client, exit, clientWire, exitWire := sessionPair(t)
	client.handshakeTimeout = 3 * time.Second
	exitWire.mu.Lock()
	exitWire.drop = true
	exitWire.mu.Unlock()
	if err := exit.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { _ = client.Start() }()
	time.Sleep(2500 * time.Millisecond)
	exitWire.mu.Lock()
	answers := len(exitWire.packets)
	exitWire.mu.Unlock()
	clientWire.mu.Lock()
	hellos := len(clientWire.packets)
	clientWire.mu.Unlock()
	// ~10 hellos in 2.5 s; the exit answers the first and then once a second.
	if answers > 4 || hellos < 6 {
		t.Fatalf("%d hellos answered %d times; want at most 4 answers", hellos, answers)
	}
}
