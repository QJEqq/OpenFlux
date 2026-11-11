package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// The augmenter keeps the node's core current itself: it compares the core
// /health reports with its own and PUTs its own when they differ, so a core
// update never needs the node image rebuilt. A pushed core is kept under
// dir/.core (a dot name is never a channel's) and is the one channels start
// on, after a container restart too, until the augmenter pushes another.

const (
	coreDir = ".core"
	maxCore = 256 << 20 // the real core is ~30 MB
)

var shaRe = regexp.MustCompile(`^[0-9a-f]{64}$`)

// coreNow is the core a channel starts on now, and its sha256.
func (a *agent) coreNow() (bin, sum string) {
	a.coreMu.RLock()
	defer a.coreMu.RUnlock()
	return a.bin, a.core
}

func (a *agent) setCore(bin, sum string) {
	a.coreMu.Lock()
	a.bin, a.core = bin, sum
	a.coreMu.Unlock()
}

// coreFrom says where the core in use came from: "pushed" or "image".
func (a *agent) coreFrom() string {
	if bin, _ := a.coreNow(); bin != a.imageBin {
		return "pushed"
	}
	return "image"
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// pushedCore is the core the augmenter pushed last, if it is still whole.
func (a *agent) pushedCore() (path, sum string, ok bool) {
	raw, err := os.ReadFile(filepath.Join(a.dir, coreDir, "current"))
	if err != nil {
		return "", "", false
	}
	sum = strings.TrimSpace(string(raw))
	if !shaRe.MatchString(sum) {
		return "", "", false
	}
	path = filepath.Join(a.dir, coreDir, "openflux-"+sum)
	if got, err := fileSHA256(path); err != nil || got != sum {
		log.Printf("[ofnode] залитое ядро %.12s… пропало или повреждено — беру ядро образа", sum)
		return "", "", false
	}
	return path, sum, true
}

// pushCore installs a core the augmenter sent and moves every channel onto
// it. The core must hash to want and run here (`--features`); if a channel
// does not come up on it, the channels go back to the core they ran on and
// the push fails. It returns how many channels it restarted.
func (a *agent) pushCore(body io.Reader, want string) (int, error) {
	cdir := filepath.Join(a.dir, coreDir)
	if err := os.MkdirAll(cdir, 0o700); err != nil {
		return 0, err
	}
	tmp, err := os.CreateTemp(cdir, "incoming-")
	if err != nil {
		return 0, err
	}
	defer os.Remove(tmp.Name()) // nothing left once it is renamed
	h := sha256.New()
	_, err = io.Copy(io.MultiWriter(tmp, h), body)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return 0, fmt.Errorf("приём ядра: %w", err)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != want {
		return 0, fmt.Errorf("SHA ядра не совпал: пришло %.12s…, ждали %.12s…", got, want)
	}
	if err := os.Chmod(tmp.Name(), 0o755); err != nil {
		return 0, err
	}
	if err := checkCore(tmp.Name()); err != nil {
		return 0, err
	}
	path := filepath.Join(cdir, "openflux-"+want)
	if err := os.Rename(tmp.Name(), path); err != nil {
		return 0, err
	}

	prevBin, prevSum := a.coreNow()
	a.setCore(path, want)
	n, err := a.restartAll()
	if err != nil {
		a.setCore(prevBin, prevSum)
		if _, rerr := a.restartAll(); rerr != nil {
			log.Printf("[ofnode] и на прежнем ядре не всё поднялось: %v", rerr)
		}
		os.Remove(path)
		return 0, fmt.Errorf("на новом ядре каналы не поднялись, вернул прежнее: %w", err)
	}
	if err := writeAtomic(filepath.Join(cdir, "current"), []byte(want+"\n")); err != nil {
		log.Printf("[ofnode] ядро %.12s… работает, но не запомнено до рестарта: %v", want, err)
	}
	a.pruneCores(path)
	return n, nil
}

// checkCore runs a core with --features (it prints what it can do and
// exits): a binary for another arch, a cut download or not a core at all
// fails here, before any channel moves onto it.
func checkCore(path string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, path, "--features").CombinedOutput()
	if err != nil {
		lines := strings.Split(strings.TrimSpace(string(out)), "\n")
		return fmt.Errorf("ядро не запускается на ноде (%v): %s", err, lines[len(lines)-1])
	}
	return nil
}

// restartAll restarts every channel's core (onto the core in use) and waits
// for each to stay up. It returns how many it restarted and the first that
// did not come up.
func (a *agent) restartAll() (int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	fresh := make([]*channel, 0, len(a.chans))
	for _, c := range a.chans {
		fresh = append(fresh, c)
	}
	for i, c := range fresh {
		close(c.stop)
		<-c.done
		nc := newChannel(c.name, c.dir, c.port, c.mode)
		a.chans[c.name] = nc
		go a.supervise(nc)
		fresh[i] = nc
	}
	for _, c := range fresh {
		if err := a.waitUp(c); err != nil {
			return len(fresh), fmt.Errorf("%s: %w", c.name, err)
		}
	}
	return len(fresh), nil
}

// pruneCores drops every pushed core but keep, the one in use.
func (a *agent) pruneCores(keep string) {
	cdir := filepath.Join(a.dir, coreDir)
	entries, _ := os.ReadDir(cdir)
	for _, e := range entries {
		if p := filepath.Join(cdir, e.Name()); e.Name() != "current" && p != keep {
			os.Remove(p)
		}
	}
}

func writeAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// handleCore takes a core from the augmenter: the body is the binary,
// X-Core-SHA256 its sha256.
func (a *agent) handleCore(w http.ResponseWriter, r *http.Request) {
	want := strings.ToLower(strings.TrimSpace(r.Header.Get("X-Core-SHA256")))
	if !shaRe.MatchString(want) {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "нужен X-Core-SHA256: sha256 ядра, 64 hex"})
		return
	}
	body := http.MaxBytesReader(w, r.Body, maxCore)
	a.pushMu.Lock()
	defer a.pushMu.Unlock()
	_, was := a.coreNow()
	if was == want {
		_, _ = io.Copy(io.Discard, body)
		writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "core": want, "restarted": 0})
		return
	}
	n, err := a.pushCore(body, want)
	if err != nil {
		log.Printf("[ofnode] ядро %.12s… не принято: %v", want, err)
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": err.Error()})
		return
	}
	log.Printf("[ofnode] ядро %.12s… → %.12s… (от аугментера), каналов перезапущено: %d", was, want, n)
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "core": want, "was": was, "restarted": n})
}
