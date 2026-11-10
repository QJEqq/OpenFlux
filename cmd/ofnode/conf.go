package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// channelSpec is what the augmenter asks the node for: the same fields its
// SSH path feeds node-install.sh, as JSON.
type channelSpec struct {
	Channel    string          `json:"channel"`
	Key        string          `json:"key"`
	Mode       string          `json:"mode"` // "l3" or "l4" (default l4)
	Transports []transportSpec `json:"transports"`
	Cookies    string          `json:"cookies"`    // base64 cookie-store JSON, for a Yandex/Mail.ru document
	Priorities map[string]int  `json:"priorities"` // carrier type -> priority; missing types keep the defaults
}

type transportSpec struct {
	Type string `json:"type"`
	URL  string `json:"url"`
}

// The carrier types a channel can carry besides direct, in the order
// node-install.sh writes them, with its default priorities.
var transportOrder = []string{"vyandex", "yandex", "mailru", "telemost", "boards", "avito", "cupsonline"}

var defaultPriority = map[string]int{
	"vyandex": 100, "yandex": 95, "mailru": 90, "telemost": 85,
	"boards": 80, "avito": 75, "cupsonline": 70, "direct": 50,
}

var (
	channelRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,30}$`)
	keyRe     = regexp.MustCompile(`^[0-9a-f]{64}$`)
	roomsRe   = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
	// What node-install.sh accepts (valid_url, valid_yandex, ...). A Volga
	// document keeps ?source=docs: without it docs.yandex.ru opens the legacy
	// editor and vyandex fails with "action_url missing".
	urlRe = map[string]*regexp.Regexp{
		"vyandex":  regexp.MustCompile(`^https://(docs|disk)\.yandex\.(ru|com|by|kz|uz)/edit/d/[A-Za-z0-9_-]{16,200}(\?source=docs)?$`),
		"yandex":   regexp.MustCompile(`^https://(docs|disk)\.yandex\.(ru|com|by|kz|uz)/(edit/d/[A-Za-z0-9_-]{16,200}|i/[A-Za-z0-9_-]{6,64})$`),
		"mailru":   regexp.MustCompile(`^https://cloud\.mail\.ru/public/[A-Za-z0-9_-]{2,64}/[A-Za-z0-9_-]{2,128}$`),
		"telemost": regexp.MustCompile(`^https://telemost\.yandex\.ru/j/[0-9]{6,32}$`),
		"boards":   regexp.MustCompile(`^https://boards\.yandex\.ru/whiteboard/\?hash=[0-9a-f]{32}$`),
	}
)

// validate checks a spec the way node-install.sh checks its config.
func (s *channelSpec) validate() error {
	if !channelRe.MatchString(s.Channel) {
		return fmt.Errorf("имя канала: ^[a-z0-9][a-z0-9-]{0,30}$")
	}
	if !keyRe.MatchString(s.Key) {
		return fmt.Errorf("ключ канала: 64 hex")
	}
	switch s.Mode {
	case "":
		s.Mode = "l4"
	case "l3", "l4":
	default:
		return fmt.Errorf("режим ноды: l4 или l3")
	}
	seen := map[string]bool{}
	doc := false
	for _, t := range s.Transports {
		if _, ok := defaultPriority[t.Type]; !ok || t.Type == "direct" {
			return fmt.Errorf("транспорт %q нельзя выбрать для ноды (direct добавляется сам)", t.Type)
		}
		if seen[t.Type] {
			return fmt.Errorf("транспорт %s выбран дважды", t.Type)
		}
		seen[t.Type] = true
		switch t.Type {
		case "avito", "cupsonline":
			if len(t.URL) < 8 || len(t.URL) > 4096 || !roomsRe.MatchString(t.URL) {
				return fmt.Errorf("неверный список комнат %s", t.Type)
			}
		default:
			if !urlRe[t.Type].MatchString(t.URL) {
				return fmt.Errorf("неверная ссылка %s: %s", t.Type, t.URL)
			}
		}
		if t.Type == "vyandex" || t.Type == "yandex" || t.Type == "mailru" {
			doc = true
		}
		if strings.ContainsAny(t.URL, "\r\n#;") {
			return fmt.Errorf("ссылка %s не помещается в node.conf", t.Type)
		}
	}
	if s.Cookies != "" {
		if !doc {
			return fmt.Errorf("вход нужен только каналу с документом Яндекса или Mail.ru")
		}
		if len(s.Cookies) > 65536 {
			return fmt.Errorf("cookies слишком большие")
		}
		raw, err := base64.StdEncoding.DecodeString(s.Cookies)
		var store map[string]map[string]string
		if err != nil || json.Unmarshal(raw, &store) != nil {
			return fmt.Errorf("cookies должны быть base64 от cookie-store JSON")
		}
	}
	for typ, p := range s.Priorities {
		if _, ok := defaultPriority[typ]; !ok {
			return fmt.Errorf("приоритет: неизвестный транспорт %q", typ)
		}
		if p < 0 || p > 999 {
			return fmt.Errorf("приоритет %s: целое 0-999", typ)
		}
	}
	return nil
}

func (s *channelSpec) priority(typ string) int {
	if p, ok := s.Priorities[typ]; ok {
		return p
	}
	return defaultPriority[typ]
}

// nodeConf is the channel's node.conf, written exactly as node-install.sh's
// write_node_conf does, so the core and the augmenter's link see the same
// channel whichever way the node was set up.
func nodeConf(s *channelSpec, dir string, port int) string {
	urls := map[string]string{}
	for _, t := range s.Transports {
		urls[t.Type] = t.URL
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# OpenFlux node channel %s, written by ofnode\nRole = exit\nMode = %s\n", s.Channel, s.Mode)
	fmt.Fprintf(&b, "EncryptionKeyFile = %s\nCookieStore = %s\n", filepath.Join(dir, "encryption-key"), filepath.Join(dir, "cookies.json"))
	// The legacy KDF context node-install.sh derives (its session_context): the
	// first document carrier. Current peers use the fixed placeholder; this
	// keeps older ones in reach.
	for _, typ := range []string{"vyandex", "yandex", "mailru", "telemost", "boards", "avito"} {
		if u := urls[typ]; u != "" {
			fmt.Fprintf(&b, "URL = %s\n", u)
			break
		}
	}
	for _, typ := range transportOrder {
		if u := urls[typ]; u != "" {
			fmt.Fprintf(&b, "\n[Transport %s]\nType = %s\nPriority = %d\nURL = %s\n", typ, typ, s.priority(typ), u)
		}
	}
	fmt.Fprintf(&b, "\n[Transport direct]\nType = direct\nPriority = %d\nListen = 0.0.0.0:%d\n", s.priority("direct"), port)
	return b.String()
}

// parseRange reads "lo-hi" (the ports the host firewall lets in for direct).
func parseRange(s string) (int, int, error) {
	a, b, ok := strings.Cut(strings.TrimSpace(s), "-")
	lo, err1 := strconv.Atoi(a)
	hi, err2 := strconv.Atoi(b)
	if !ok || err1 != nil || err2 != nil || lo < 1024 || hi > 65535 || lo > hi {
		return 0, 0, fmt.Errorf("PORT_RANGE %q: нужно lo-hi в 1024-65535", s)
	}
	return lo, hi, nil
}

// pickPort finds a free TCP port in [lo, hi] that no channel holds.
func pickPort(lo, hi int, taken map[int]bool) (int, error) {
	for i := 0; i < 200; i++ {
		p := lo + rand.IntN(hi-lo+1)
		if taken[p] {
			continue
		}
		l, err := net.Listen("tcp", ":"+strconv.Itoa(p))
		if err != nil {
			continue
		}
		l.Close()
		return p, nil
	}
	return 0, fmt.Errorf("нет свободного порта в %d-%d", lo, hi)
}
