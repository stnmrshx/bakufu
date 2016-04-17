/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package ssl

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io/ioutil"
	nethttp "net/http"
	"strings"

	"github.com/go-martini/martini"
	"github.com/stnmrshx/golib/log"
	"github.com/stnmrshx/bakufu/go/config"
)

var cipherSuites = []uint16{
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
	tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
	tls.TLS_RSA_WITH_AES_128_CBC_SHA,
	tls.TLS_RSA_WITH_AES_256_CBC_SHA,
}

func HasString(elem string, arr []string) bool {
	for _, s := range arr {
		if s == elem {
			return true
		}
	}
	return false
}

func NewTLSConfig(caFile string, mutualTLS bool) (*tls.Config, error) {
	var c tls.Config

	c.MinVersion = tls.VersionTLS12
	c.CipherSuites = cipherSuites
	c.PreferServerCipherSuites = true

	if mutualTLS {
		log.Info("MutualTLS requested, client certificates will be verified")
		c.ClientAuth = tls.VerifyClientCertIfGiven
	}
	if caFile != "" {
		data, err := ioutil.ReadFile(caFile)
		if err != nil {
			return &c, err
		}
		c.ClientCAs = x509.NewCertPool()
		if !c.ClientCAs.AppendCertsFromPEM(data) {
			return &c, errors.New("No certificates parsed")
		}
		log.Info("Read in CA file:", caFile)
	}
	c.BuildNameToCertificate()
	return &c, nil
}

func Verify(r *nethttp.Request, validOUs []string) error {
	if strings.Contains(r.URL.String(), config.Config.StatusEndpoint) && !config.Config.StatusOUVerify {
		return nil
	}
	if r.TLS == nil {
		return errors.New("No TLS")
	}
	for _, chain := range r.TLS.VerifiedChains {
		s := chain[0].Subject.OrganizationalUnit
		log.Debug("All OUs:", strings.Join(s, " "))
		for _, ou := range s {
			log.Debug("Client presented OU:", ou)
			if HasString(ou, validOUs) {
				log.Debug("Found valid OU:", ou)
				return nil
			}
		}
	}
	log.Error("No valid OUs found")
	return errors.New("Invalid OU")
}

func VerifyOUs(validOUs []string) martini.Handler {
	return func(res nethttp.ResponseWriter, req *nethttp.Request, c martini.Context) {
		log.Debug("Verifying client OU")
		if err := Verify(req, validOUs); err != nil {
			nethttp.Error(res, err.Error(), nethttp.StatusUnauthorized)
		}
	}
}

func AppendKeyPair(tlsConfig *tls.Config, certFile string, keyFile string) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}
	tlsConfig.Certificates = append(tlsConfig.Certificates, cert)
	return nil
}

func ListenAndServeTLS(addr string, handler nethttp.Handler, tlsConfig *tls.Config) error {
	if addr == "" {
		addr = ":https"
	}
	l, err := tls.Listen("tcp", addr, tlsConfig)
	if err != nil {
		return err
	}
	return nethttp.Serve(l, handler)
}
