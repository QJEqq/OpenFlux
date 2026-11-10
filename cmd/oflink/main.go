// Command oflink builds a channel's openflux:// link from its key, node
// address/port and transports, via provision.ShareLinkContext — so the link
// matches exactly what node-install.sh wrote into node.conf (priorities +
// encryption context). The AVACA companion shells out to this after it has
// applied a channel on the node and read the port.
//
// Usage:
//
//	oflink --name <channel> --key <64hex> --host <node> --port <n> \
//	       [--transports '[{"type":"mailru","url":"https://cloud.mail.ru/public/.../..."}]'] \
//	       [--context <ctx>]
//
// Prints the openflux:// link on stdout, or an error on stderr with exit 1.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/p1neappleXpress/OpenFlux/provision"
	"github.com/p1neappleXpress/OpenFlux/share"
)

func main() {
	name := flag.String("name", "", "channel name (node-install.sh channel)")
	key := flag.String("key", "", "channel key, 64 hex")
	host := flag.String("host", "", "node host for the direct backup")
	port := flag.Int("port", 0, "direct backup port the node picked")
	context := flag.String("context", "", "encryption context; empty derives it (new channel)")
	transportsJSON := flag.String("transports", "[]", `JSON array of {"type","url"} besides direct`)
	prioritiesFlag := flag.String("priorities", "", `carrier priorities, e.g. "direct:100,boards:70,vyandex:50" (empty: provision's)`)
	flag.Parse()

	var ts []provision.ChannelTransport
	if err := json.Unmarshal([]byte(*transportsJSON), &ts); err != nil {
		fmt.Fprintln(os.Stderr, "bad --transports json:", err)
		os.Exit(1)
	}
	prios, err := parsePriorities(*prioritiesFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bad --priorities:", err)
		os.Exit(1)
	}

	link, err := provision.ShareLinkContext(*name, *key, *host, *port, ts, *context)
	if err == nil {
		link, err = adjustLink(link, ts, prios)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	fmt.Println(link)
}

// parsePriorities reads "type:N,type:N" (N 0-999) into a map.
func parsePriorities(s string) (map[string]int, error) {
	out := map[string]int{}
	for _, part := range strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ' ' }) {
		typ, num, ok := strings.Cut(part, ":")
		n, err := strconv.Atoi(num)
		if !ok || typ == "" || err != nil || n < 0 || n > 999 {
			return nil, fmt.Errorf("%q: нужно тип:число (0-999)", part)
		}
		out[typ] = n
	}
	return out, nil
}

// adjustLink applies what the AVACA augmenter's node-install.sh writes into
// node.conf and provision does not, so the link and node.conf agree:
//
//   - ?source=docs goes back on the vyandex carrier when the caller's URL had
//     it. provision cleans every carrier link down to /edit/d/<id>, but without
//     that query docs.yandex.ru opens the document in the legacy editor, whose
//     page has no officeActionData, and vyandex fails with "action_url missing".
//   - the carrier priorities the augmenter chose (--priorities). A Session sends
//     through the live carrier of the highest priority, so the order decides
//     which carrier takes the traffic (direct, when live, wins anyway).
//
// The encryption context does not depend on carrier URLs or priorities
// (explicit, or the fixed placeholder), so nothing else in the link changes.
func adjustLink(link string, ts []provision.ChannelTransport, prios map[string]int) (string, error) {
	keep := false
	for _, t := range ts {
		if u, err := url.Parse(strings.TrimSpace(t.URL)); t.Type == "vyandex" && err == nil && u.Query().Get("source") == "docs" {
			keep = true
		}
	}
	if !keep && len(prios) == 0 {
		return link, nil
	}
	c, err := share.Decode(link)
	if err != nil {
		return "", err
	}
	for i, t := range c.Transports {
		if keep && t.Type == "vyandex" && !strings.Contains(t.URL, "?") {
			c.Transports[i].URL += "?source=docs"
		}
		if p, ok := prios[t.Type]; ok {
			c.Transports[i].Priority = p
		}
	}
	return share.MakeLink(c)
}
