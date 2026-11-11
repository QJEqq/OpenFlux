package yandex

import (
	"net/url"
	"strings"
)

// isPassportRefresh reports whether loc is Passport's session refresh,
// passport.yandex.<tld>/auth/update/?retpath=…, rather than its login form.
// Docs sends a signed-in visitor there when the sign-in is still good but its
// cookies are due to be reissued: Passport sets fresh Session_id/sessionid2
// on .yandex.ru and redirects back to retpath. A browser passes it unnoticed,
// so the transports follow it once the same way instead of reporting a login
// wall (an exit has no WebView to clear one). A sign-in Passport will not
// refresh lands on its login form, which is the wall.
func isPassportRefresh(loc string) bool {
	if !strings.Contains(loc, "passport.yandex") {
		return false
	}
	u, err := url.Parse(loc)
	if err != nil {
		return false
	}
	return strings.HasSuffix(strings.TrimSuffix(u.Path, "/"), "/auth/update")
}
