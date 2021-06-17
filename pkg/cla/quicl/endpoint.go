// SPDX-FileCopyrightText: 2021 Markus Sommer
//
// SPDX-License-Identifier: GPL-3.0-or-later

package quicl

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/dtn7/cboring"
	"io"
	"net"
	"time"

	"github.com/dtn7/dtn7-go/pkg/bpv7"
	"github.com/dtn7/dtn7-go/pkg/cla"
	"github.com/dtn7/dtn7-go/pkg/cla/quicl/internal"
	"github.com/lucas-clemente/quic-go"
	log "github.com/sirupsen/logrus"
)

type Endpoint struct {
	id          bpv7.EndpointID
	peerId      bpv7.EndpointID
	peerAddress string
	session     quic.Session

	reportingChannel chan cla.ConvergenceStatus

	permanent bool
	dialer    bool
}

func NewListenerEndpoint(id bpv7.EndpointID, session quic.Session) *Endpoint {
	return &Endpoint{
		id:               id,
		peerAddress:      session.RemoteAddr().String(),
		session:          session,
		reportingChannel: make(chan cla.ConvergenceStatus),
		permanent:        false,
		dialer:           false,
	}
}

func NewDialerEndpoint(peerAddress string, id bpv7.EndpointID, permanent bool) *Endpoint {
	return &Endpoint{
		id:               id,
		peerAddress:      peerAddress,
		reportingChannel: make(chan cla.ConvergenceStatus),
		permanent:        permanent,
		dialer:           true,
	}
}

/**
Methods for Convergable interface
*/

func (endpoint *Endpoint) Close() error {
	// TODO: Dummy Implementation
	return nil
}

/**
Methods for Convergence interface
*/

func (endpoint *Endpoint) Start() (error, bool) {
	// if we are on the dialer-side we need to first initiate the quic-session
	if endpoint.dialer {
		session, err := quic.DialAddr(endpoint.peerAddress, internal.GenerateDialerTLSConfig(), internal.GenerateQUICConfig())
		endpoint.session = session
		if err != nil {
			return err, endpoint.permanent
		}
	}

	log.WithFields(log.Fields{
		"endpoint": endpoint.id,
		"peer":     endpoint.peerAddress,
	}).Debug("Starting CLA")

	var err error
	if endpoint.dialer {
		err = endpoint.handshakeDialer()
	} else {
		err = endpoint.handshakeListener()
	}

	if err != nil {
		var herr *internal.HandshakeError
		if errors.As(err, &herr) {
			log.WithFields(log.Fields{
				"cla":      endpoint,
				"error":    herr,
				"internal": herr.Unwrap(),
			}).Warn("Handshake failure")
			_ = endpoint.session.CloseWithError(herr.Code, herr.Msg)
		} else {
			log.WithFields(log.Fields{
				"cla":   endpoint,
				"error": err,
			}).Error("Non handshake related error during handshake")
			_ = endpoint.session.CloseWithError(internal.LocalError, "Local error")
		}
	}
	return err, endpoint.permanent
}

func (endpoint *Endpoint) Channel() chan cla.ConvergenceStatus {
	return endpoint.reportingChannel
}

func (endpoint *Endpoint) Address() string {
	return endpoint.peerAddress
}

func (endpoint *Endpoint) IsPermanent() bool {
	return endpoint.permanent
}

/**
Methods for ConvergenceReceiver interface
*/

func (endpoint *Endpoint) GetEndpointID() bpv7.EndpointID {
	return endpoint.id
}

/**
Methods for ConvergenceSender interface
*/

func (endpoint *Endpoint) GetPeerEndpointID() bpv7.EndpointID {
	return endpoint.peerId
}

func (endpoint *Endpoint) Send(bndl bpv7.Bundle) error {
	stream, err := endpoint.session.OpenStream()
	if err != nil {
		// TODO: understand possible error cases
		return err
	}

	buff := new(bytes.Buffer)
	if err = cboring.Marshal(&bndl, buff); err != nil {
		stream.CancelWrite(internal.DataMarshalError)
		_ = stream.Close()
		return err
	}

	writer := bufio.NewWriter(stream)
	if err = cboring.WriteByteStringLen(uint64(buff.Len()), writer); err != nil {
		stream.CancelWrite(internal.StreamTransmissionError)
		_ = stream.Close()
		return err
	}

	if _, err = buff.WriteTo(writer); err != nil {
		stream.CancelWrite(internal.StreamTransmissionError)
		_ = stream.Close()
		return err
	}

	if err = writer.Flush(); err != nil {
		stream.CancelWrite(internal.StreamTransmissionError)
		_ = stream.Close()
		return err
	}

	_ = stream.Close()
	return nil
}

/*
Non-interface methods
*/

func (endpoint *Endpoint) String() string {
	return fmt.Sprintf("QUICLEndpoint{Peer ID: %v, Peer Address: %v, Dialer: %v, Permanent: %v}", endpoint.peerId, endpoint.peerAddress, endpoint.dialer, endpoint.permanent)
}

func (endpoint *Endpoint) accept() {
	for {
		_, err := endpoint.session.AcceptStream(context.Background())
		if err != nil {
			if nerr, ok := err.(net.Error); ok {
				if nerr.Timeout() {
					log.WithFields(log.Fields{
						"CLA":   endpoint,
						"error": nerr,
					}).Debug("Peer timed out.")

					endpoint.reportPeerDisappeared()

					return
				}
			} else {
				log.WithFields(log.Fields{
					"CLA":   endpoint,
					"error": err,
				}).Error("Unexpected error while waiting for stream")
			}
		}
	}
}

func (endpoint *Endpoint) handshakeListener() error {
	log.WithField("cla", endpoint.peerAddress).Debug("Performing handshake")

	// the dialer has half a second to initiate the handshake
	timeout := 500 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// wait for the dialer to open a stream
	stream, err := endpoint.session.AcceptStream(ctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return internal.NewHandshakeError("dialer took too long to initiate handshake", internal.PeerError, err)
		} else {
			return internal.NewHandshakeError("unanticipated error happened", internal.UnknownError, err)
		}
	}

	// the listener first receives the dialer's id
	if err = endpoint.receiveEndpointID(stream); err != nil {
		return err
	}

	// then sen our own id
	if err = endpoint.sendEndpointID(stream); err != nil {
		return err
	}

	// lastly, close the stream
	if err = stream.Close(); err != nil {
		return internal.NewHandshakeError("error closing handshake stream", internal.ConnectionError, err)
	}

	return nil
}

func (endpoint *Endpoint) handshakeDialer() error {
	log.WithField("cla", endpoint.peerAddress).Debug("Performing handshake")

	stream, err := endpoint.session.OpenStream()
	if err != nil {
		return internal.NewHandshakeError("Error during stream initiation", internal.ConnectionError, err)
	}

	// start by sending own ID
	err = endpoint.sendEndpointID(stream)
	if err != nil {
		return err
	}

	// wait for our peer's ID
	err = endpoint.receiveEndpointID(stream)

	return err
}

func (endpoint *Endpoint) sendEndpointID(stream quic.Stream) error {
	log.WithField("cla", endpoint).Debug("Sending own endpoint id")

	buff := new(bytes.Buffer)
	if err := cboring.Marshal(&endpoint.id, buff); err != nil {
		return internal.NewHandshakeError("error marshaling endpoint-id", internal.LocalError, err)
	}

	writer := bufio.NewWriter(stream)
	if err := cboring.WriteByteStringLen(uint64(buff.Len()), writer); err != nil {
		return internal.NewHandshakeError("error sending id length", internal.ConnectionError, err)
	}

	if _, err := buff.WriteTo(writer); err != nil {
		return internal.NewHandshakeError("error sending id", internal.ConnectionError, err)
	}

	if err := writer.Flush(); err != nil {
		return internal.NewHandshakeError("error flushing write-buffer", internal.ConnectionError, err)
	}

	return nil
}

func (endpoint *Endpoint) receiveEndpointID(stream quic.Stream) error {
	log.WithField("cla", endpoint).Debug("Receiving peer's endpoint id")
	reader := bufio.NewReader(stream)

	length, err := cboring.ReadByteStringLen(reader)
	if err != nil && !errors.Is(err, io.EOF) {
		return internal.NewHandshakeError("error reading id length", internal.ConnectionError, err)
	} else if length == 0 {
		return internal.NewHandshakeError("error reading id length", internal.ConnectionError, fmt.Errorf("length is 0"))
	}

	id := new(bpv7.EndpointID)
	if err = cboring.Unmarshal(id, reader); err != nil {
		// TODO: distinguish cbor and transmission erros
		return internal.NewHandshakeError("error reading id", internal.ConnectionError, err)
	}

	log.WithFields(log.Fields{
		"cla":     endpoint,
		"peer id": id,
	}).Debug("Received peer's endpoint id")

	endpoint.peerId = *id

	return nil
}

func (endpoint *Endpoint) reportPeerDisappeared() {
	// TODO: Report peer disappearance
}
