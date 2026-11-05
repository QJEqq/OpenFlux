package mailru

import "github.com/p1neappleXpress/OpenFlux/transport"

// MailruDocsTransport must satisfy ErrorNotifier so the manager/mobile layer
// wires its login prompt by type assertion. If SetErrorNotifier's signature
// drifts from the interface, the assertion below fails to compile — rather than
// silently leaving the transport un-wired and the WebView prompt never raised.
var _ transport.ErrorNotifier = (*MailruDocsTransport)(nil)
