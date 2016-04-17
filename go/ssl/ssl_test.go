/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package ssl_test

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	nethttp "net/http"
	"strings"
	"syscall"
	"testing"

	"github.com/stnmrshx/bakufu/go/config"
	"github.com/stnmrshx/bakufu/go/ssl"
)

func TestHasString(t *testing.T) {
	elem := "foo"
	a1 := []string{"bar", "foo", "baz"}
	a2 := []string{"bar", "fuu", "baz"}
	good := ssl.HasString(elem, a1)
	if !good {
		t.Errorf("Didn't find %s in array %s", elem, strings.Join(a1, ", "))
	}
	bad := ssl.HasString(elem, a2)
	if bad {
		t.Errorf("Unexpectedly found %s in array %s", elem, strings.Join(a2, ", "))
	}
}

func TestNewTLSConfig(t *testing.T) {
	fakeCA := writeFakeFile(pemCertificate)
	defer syscall.Unlink(fakeCA)

	conf, err := ssl.NewTLSConfig(fakeCA, true)
	if err != nil {
		t.Errorf("Could not create new TLS config: %s", err)
	}
	if conf.ClientAuth != tls.VerifyClientCertIfGiven {
		t.Errorf("Client certificate verification was not enabled")
	}
	if conf.ClientCAs == nil {
		t.Errorf("ClientCA empty even though cert provided")
	}

	conf, err = ssl.NewTLSConfig("", false)
	if err != nil {
		t.Errorf("Could not create new TLS config: %s", err)
	}
	if conf.ClientAuth == tls.VerifyClientCertIfGiven {
		t.Errorf("Client certificate verification was enabled unexpectedly")
	}
	if conf.ClientCAs != nil {
		t.Errorf("Filling in ClientCA somehow without a cert")
	}
}

func TestStatus(t *testing.T) {
	var validOUs []string
	url := fmt.Sprintf("http://example.com%s", config.Config.StatusEndpoint)

	req, err := nethttp.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal(err)
	}
	config.Config.StatusOUVerify = false
	if err := ssl.Verify(req, validOUs); err != nil {
		t.Errorf("Failed even with verification off")
	}
	config.Config.StatusOUVerify = true
	if err := ssl.Verify(req, validOUs); err == nil {
		t.Errorf("Did not fail on with bad verification")
	}
}

func TestVerify(t *testing.T) {
	var validOUs []string

	req, err := nethttp.NewRequest("GET", "http://example.com/foo", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := ssl.Verify(req, validOUs); err == nil {
		t.Errorf("Did not fail on lack of TLS config")
	}

	pemBlock, _ := pem.Decode([]byte(pemCertificate))
	cert, err := x509.ParseCertificate(pemBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}

	var tcs tls.ConnectionState
	req.TLS = &tcs

	if err := ssl.Verify(req, validOUs); err == nil {
		t.Errorf("Found a valid OU without any being available")
	}

	cert.Subject.OrganizationalUnit = []string{"testing"}

	req.TLS.PeerCertificates = []*x509.Certificate{cert}
	req.TLS.VerifiedChains = [][]*x509.Certificate{req.TLS.PeerCertificates}

	validOUs = []string{"testing"}

	if err := ssl.Verify(req, validOUs); err != nil {
		t.Errorf("Failed to verify certificate OU")
	}
}

func TestAppendKeyPair(t *testing.T) {
	c, err := ssl.NewTLSConfig("", false)
	if err != nil {
		t.Fatal(err)
	}
	pemCertFile := writeFakeFile(pemCertificate)
	defer syscall.Unlink(pemCertFile)
	pemPKFile := writeFakeFile(pemPrivateKey)
	defer syscall.Unlink(pemPKFile)

	if err := ssl.AppendKeyPair(c, pemCertFile, pemPKFile); err != nil {
		t.Errorf("Failed to append certificate and key to tls config")
	}
}

func writeFakeFile(content string) string {
	f, err := ioutil.TempFile("", "ssl_test")
	if err != nil {
		return ""
	}
	ioutil.WriteFile(f.Name(), []byte(content), 0644)
	return f.Name()
}

const pemCertificate = `-----BEGIN CERTIFICATE-----
MIIB5DCCAZCgAwIBAgIBATALBgkqhkiG9w0BAQUwLTEQMA4GA1UEChMHQWNtZSBDbzEZMBcGA1UE
AxMQdGVzdC5leGFtcGxlLmNvbTAeFw03MDAxMDEwMDE2NDBaFw03MDAxMDIwMzQ2NDBaMC0xEDAO
BgNVBAoTB0FjbWUgQ28xGTAXBgNVBAMTEHRlc3QuZXhhbXBsZS5jb20wWjALBgkqhkiG9w0BAQED
SwAwSAJBALKZD0nEffqM1ACuak0bijtqE2QrI/KLADv7l3kK3ppMyCuLKoF0fd7Ai2KW5ToIwzFo
fvJcS/STa6HA5gQenRUCAwEAAaOBnjCBmzAOBgNVHQ8BAf8EBAMCAAQwDwYDVR0TAQH/BAUwAwEB
/zANBgNVHQ4EBgQEAQIDBDAPBgNVHSMECDAGgAQBAgMEMBsGA1UdEQQUMBKCEHRlc3QuZXhhbXBs
ZS5jb20wDwYDVR0gBAgwBjAEBgIqAzAqBgNVHR4EIzAhoB8wDoIMLmV4YW1wbGUuY29tMA2CC2V4
YW1wbGUuY29tMAsGCSqGSIb3DQEBBQNBAHKZKoS1wEQOGhgklx4+/yFYQlnqwKXvar/ZecQvJwui
0seMQnwBhwdBkHfVIU2Fu5VUMRyxlf0ZNaDXcpU581k=
-----END CERTIFICATE-----`

const pemPrivateKey = `-----BEGIN RSA PRIVATE KEY-----
MIIBOgIBAAJBALKZD0nEffqM1ACuak0bijtqE2QrI/KLADv7l3kK3ppMyCuLKoF0
fd7Ai2KW5ToIwzFofvJcS/STa6HA5gQenRUCAwEAAQJBAIq9amn00aS0h/CrjXqu
/ThglAXJmZhOMPVn4eiu7/ROixi9sex436MaVeMqSNf7Ex9a8fRNfWss7Sqd9eWu
RTUCIQDasvGASLqmjeffBNLTXV2A5g4t+kLVCpsEIZAycV5GswIhANEPLmax0ME/
EO+ZJ79TJKN5yiGBRsv5yvx5UiHxajEXAiAhAol5N4EUyq6I9w1rYdhPMGpLfk7A
IU2snfRJ6Nq2CQIgFrPsWRCkV+gOYcajD17rEqmuLrdIRexpg8N1DOSXoJ8CIGlS
tAboUGBxTDq3ZroNism3DaMIbKPyYrAqhKov1h5V
-----END RSA PRIVATE KEY-----
`
