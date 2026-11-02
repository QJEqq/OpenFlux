// Command oflink builds a channel's openflux:// link from its key, node
// address/port and transports, via provision.ShareLinkContext — so the link
// matches exactly what node-install.sh wrote into node.conf (priorities +
// encryption context). The AVACA companion shells out to this after it has
// applied a channel on the node and read the port.
//
// Usage:
//   oflink --name <channel> --key <64hex> --host <node> --port <n> \
//          [--transports '[{"type":"mailru","url":"https://cloud.mail.ru/public/.../..."}]'] \
//          [--context <ctx>]
// Prints the openflux:// link on stdout, or an error on stderr with exit 1.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/p1neappleXpress/OpenFlux/provision"
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
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	fmt.Println(link)
}
