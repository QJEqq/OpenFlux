package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"time"
)

// secretBundle is what SECRET_KEY carries to the node, remnanode-style: the
// API token and the node's own TLS certificate and key. The augmenter makes
// it (ofnode secret), keeps the certificate and the token, and puts the whole
// bundle into the node's docker-compose.yml — so it already trusts the node's
// certificate (pinned, no CA, no hostname) and nothing has to come back from
// the node by hand.
type secretBundle struct {
	Token string `json:"token"`
	Cert  string `json:"cert"` // PEM
	Key   string `json:"key"`  // PEM (EC private key)
}

// certName is the one name in the node certificate; clients pin the
// certificate itself, so the address the node is reached at does not matter.
const certName = "openflux-node"

func newSecret() (secretBundle, error) {
	tok := make([]byte, 32)
	if _, err := rand.Read(tok); err != nil {
		return secretBundle{}, err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return secretBundle{}, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		return secretBundle{}, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: certName},
		DNSNames:     []string{certName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().AddDate(20, 0, 0),
		// Self-signed and its own trust anchor: OpenSSL (the augmenter's Python
		// client) wants a CA certificate in its trust store.
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return secretBundle{}, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return secretBundle{}, err
	}
	return secretBundle{
		Token: hex.EncodeToString(tok),
		Cert:  string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		Key:   string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})),
	}, nil
}

// encode is the SECRET_KEY value: base64 of the bundle's JSON (no quoting
// trouble in a compose file).
func (b secretBundle) encode() string {
	raw, _ := json.Marshal(b)
	return base64.StdEncoding.EncodeToString(raw)
}

func parseSecret(s string) (secretBundle, tls.Certificate, error) {
	var b secretBundle
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil || json.Unmarshal(raw, &b) != nil || len(b.Token) < 32 {
		return b, tls.Certificate{}, fmt.Errorf("SECRET_KEY: не base64 от {token, cert, key} — возьми его из аугментера")
	}
	cert, err := tls.X509KeyPair([]byte(b.Cert), []byte(b.Key))
	if err != nil {
		return b, tls.Certificate{}, fmt.Errorf("SECRET_KEY: сертификат не читается: %w", err)
	}
	return b, cert, nil
}
