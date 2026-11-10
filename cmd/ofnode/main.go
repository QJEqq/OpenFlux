// Command ofnode is the OpenFlux node agent for the AVACA augmenter — the
// remnanode-style way to add a node. The node runs one container (this agent
// and the core) from a docker-compose.yml the augmenter hands out with
// SECRET_KEY in it; the augmenter then drives the node over HTTPS instead of
// SSH: list, apply and remove channels. Each channel is one supervised
// `openflux --config node.conf` child, with the node.conf node-install.sh would
// write, kept under DATA_DIR and brought back when the container restarts.
//
//	ofnode secret   prints a new {"secret_key","cert","token"} for one node
//	ofnode serve    runs the agent; env:
//	                  SECRET_KEY    the bundle from `ofnode secret` (required)
//	                  NODE_PORT     API port (default 2223)
//	                  PORT_RANGE    ports for the channels' direct (default 20000-29999)
//	                  DATA_DIR      channel state (default /var/lib/openflux-node)
//	                  OPENFLUX_BIN  the core (default /usr/local/bin/openflux)
//
// API (HTTPS, Authorization: Bearer <token>):
//
//	GET    /health            {"ok","agent","core","arch","channels","port_range"}
//	GET    /channels          {"ok","channels":[{"channel","port","mode","state","restarts","last_exit"}]}
//	POST   /channels          {"channel","key","mode","transports":[{"type","url"}],"cookies","priorities"} -> {"ok","port"}
//	DELETE /channels/{name}   {"ok"}
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "secret":
		b, err := newSecret()
		if err != nil {
			log.Fatal(err)
		}
		_ = json.NewEncoder(os.Stdout).Encode(map[string]string{"secret_key": b.encode(), "cert": b.Cert, "token": b.Token})
	case "serve":
		if err := serve(); err != nil {
			log.Fatalf("[ofnode] %v", err)
		}
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: ofnode secret | ofnode serve   (see the package doc for the env)")
	os.Exit(2)
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func serve() error {
	sec, cert, err := parseSecret(os.Getenv("SECRET_KEY"))
	if err != nil {
		return err
	}
	port, err := strconv.Atoi(env("NODE_PORT", "2223"))
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("NODE_PORT %q", os.Getenv("NODE_PORT"))
	}
	lo, hi, err := parseRange(env("PORT_RANGE", "20000-29999"))
	if err != nil {
		return err
	}
	a, err := newAgent(env("DATA_DIR", "/var/lib/openflux-node"), env("OPENFLUX_BIN", "/usr/local/bin/openflux"), lo, hi)
	if err != nil {
		return err
	}
	a.restore()
	log.Printf("[ofnode] %s: API :%d, каналов %d, ядро %.12s…, порты direct %d-%d", agentVersion, port, len(a.chans), a.core, lo, hi)

	srv := &http.Server{
		Addr:              ":" + strconv.Itoa(port),
		Handler:           a.routes(sec.Token),
		TLSConfig:         &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
		ReadHeaderTimeout: 10 * time.Second,
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, os.Interrupt)
	go func() {
		<-sig
		log.Printf("[ofnode] остановка: гашу ядра каналов (каналы остаются на диске)")
		a.stopAll()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()
	if err := srv.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}
