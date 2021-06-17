// SPDX-FileCopyrightText: 2021 Markus Sommer
//
// SPDX-License-Identifier: GPL-3.0-or-later

package internal

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"time"

	"github.com/lucas-clemente/quic-go"

	log "github.com/sirupsen/logrus"
)

// GenerateListenerTLSConfig sets up a bare-bones TLS config for the listener
func GenerateListenerTLSConfig() *tls.Config {
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		log.WithError(err).Fatal("Error generating TLSConfig")
	}
	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		log.WithError(err).Fatal("Error generating TLSConfig")
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		log.WithError(err).Fatal("Error generating TLSConfig")
	}
	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		NextProtos:   []string{"bpv7-quicl"},
	}
}

func GenerateDialerTLSConfig() *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"bpv7-quicl"},
	}
}

func GenerateQUICConfig() *quic.Config {
	return &quic.Config{
		KeepAlive:       true,
		MaxIdleTimeout:  5 * time.Second,
		EnableDatagrams: false,
	}
}
