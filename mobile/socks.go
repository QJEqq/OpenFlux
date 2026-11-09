package mobile

import (
	"fmt"

	"github.com/p1neappleXpress/OpenFlux/socks5"
	"github.com/p1neappleXpress/OpenFlux/transport"
	"github.com/p1neappleXpress/OpenFlux/tunnel"
	"github.com/p1neappleXpress/OpenFlux/utils"
	"github.com/p1neappleXpress/OpenFlux/share" 
)
func StartSocksLink(link, socksAddr string) string {
	c, err := share.Decode(link)
	if err != nil {
		return err.Error()
	}
	return StartSocks(sessionSpecsOf(c), c.Secret, socksAddr)
}
func StartSocks(specsJSON, encryptionSecret, socksAddr string) string {
	if socksAddr == "" {
		socksAddr = "127.0.0.1:1080"
	}
	return startSocks(socksAddr, func() (transport.Transport, error) {
		return buildSession(specsJSON, encryptionSecret, false) // как StartSession, mobile.go:91
	})
}

func startSocks(socksAddr string, build func() (transport.Transport, error)) string {
	client.mu.Lock()
	if client.running {
		client.mu.Unlock()
		return ""
	}
	client.running = true
	client.logs = nil
	client.mu.Unlock()

	utils.SetLevel(int(debugLevel.Load()))
	utils.SetLogSink(appendLog)

	fail := func(err error) string {
		appendLog(fmt.Sprintf("[ERROR] Ошибка запуска: %v", err))
		client.mu.Lock()
		client.running = false
		client.mu.Unlock()
		detachPrompt() // 0.17 переименовал captcha → prompt (коммит 1fd2f23)
		setAuthProxy(nil)
		return err.Error()
	}

	trans, err := build()
	if err != nil {
		return fail(err)
	}

	tun := tunnel.NewTCPTunnel(trans, false)
	if e := tun.Err(); e != nil {
		return fail(e)
	}

	if err := trans.Start(); err != nil {
		return fail(err)
	}

	srv := socks5.NewSOCKS5Server(socksAddr, tun)
	if err := srv.Bind(); err != nil { // синхронно ловим "address in use"
		_ = trans.Stop()
		return fail(err)
	}

	client.mu.Lock()
	client.transport = trans
	client.tun = tun
	client.socks = srv
	client.mu.Unlock()

	appendLog(fmt.Sprintf("[ANDROID] SOCKS5 на %s (openflux как нода)", socksAddr))
	utils.SafeGo("mobile.socks", func() { _ = srv.Start() }) // блокируется; Close() разблокирует
	return ""

}
