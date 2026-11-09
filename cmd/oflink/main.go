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
	flag.Parse()

	var ts []provision.ChannelTransport
	if err := json.Unmarshal([]byte(*transportsJSON), &ts); err != nil {
		fmt.Fprintln(os.Stderr, "bad --transports json:", err)
		os.Exit(1)
	}

	link, err := provision.ShareLinkContext(*name, *key, *host, *port, ts, *context)
	if err == nil {
		link, err = keepVolgaSource(link, ts)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	fmt.Println(link)
}

// keepVolgaSource puts ?source=docs back on the link's vyandex carrier when the
// caller's URL had it. provision cleans every carrier link down to
// /edit/d/<id>, but without that query docs.yandex.ru opens the document in the
// legacy editor, whose page has no officeActionData, and vyandex fails with
// "action_url missing". The AVACA augmenter's node-install.sh keeps the query
// in node.conf, so the link must carry the same URL. The encryption context
// does not depend on carrier URLs (explicit, or the fixed placeholder), so
// nothing else in the link changes.
func keepVolgaSource(link string, ts []provision.ChannelTransport) (string, error) {
	keep := false
	for _, t := range ts {
		if u, err := url.Parse(strings.TrimSpace(t.URL)); t.Type == "vyandex" && err == nil && u.Query().Get("source") == "docs" {
			keep = true
		}
	}
	if !keep {
		return link, nil
	}
	c, err := share.Decode(link)
	if err != nil {
		return "", err
	}
	for i, t := range c.Transports {
		if t.Type == "vyandex" && !strings.Contains(t.URL, "?") {
			c.Transports[i].URL += "?source=docs"
		}
	}
	return share.MakeLink(c)
}
