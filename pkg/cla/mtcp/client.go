// SPDX-FileCopyrightText: 2019 Markus Sommer
// SPDX-FileCopyrightText: 2019, 2020, 2021 Alvar Penning
//
// SPDX-License-Identifier: GPL-3.0-or-later

package mtcp

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/dtn7/cboring"

	"github.com/dtn7/dtn7-go/pkg/bpv7"
	"github.com/dtn7/dtn7-go/pkg/cla"
)

// MTCPClient is an implementation of a Minimal TCP Convergence-Layer client
// which connects to a MTCP server to send bundles. This struct implements
// a ConvergenceSender.
type MTCPClient struct {
	conn       net.Conn
	peer       bpv7.EndpointID
	mutex      sync.Mutex
	reportChan chan cla.ConvergenceStatus

	permanent bool
	address   string

	stopSyn chan struct{}
	stopAck chan struct{}
}

// NewMTCPClient creates a new MTCPClient, connected to the given address for
// the registered endpoint ID. The permanent flag indicates if this MTCPClient
// should never be removed from the core.
func NewMTCPClient(address string, peer bpv7.EndpointID, permanent bool) *MTCPClient {
	return &MTCPClient{
		peer:      peer,
		permanent: permanent,
		address:   address,
	}
}

// NewAnonymousMTCPClient creates a new MTCPClient, connected to the given address.
// The permanent flag indicates if this MTCPClient should never be removed from
// the core.
func NewAnonymousMTCPClient(address string, permanent bool) *MTCPClient {
	return NewMTCPClient(address, bpv7.DtnNone(), permanent)
}

func (client *MTCPClient) Start() (err error, retry bool) {
	retry = true

	conn, connErr := dial(client.address)
	if connErr != nil {
		err = connErr
		return
	}

	client.reportChan = make(chan cla.ConvergenceStatus)
	client.stopSyn = make(chan struct{})
	client.stopAck = make(chan struct{})

	client.conn = conn

	go client.handler()
	return
}

func (client *MTCPClient) handler() {
	defer func() {
		if r := recover(); r != nil {
			log.WithField("error", r).Error("Panic in MTCPClient handler")
		}
	}()

	var ticker = time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	// Introduce ourselves once
	client.reportChan <- cla.NewConvergencePeerAppeared(client, client.GetPeerEndpointID())

	for {
		select {
		case <-client.stopSyn:
			// log.WithFields(log.Fields{
			// 	"client": client.String(),
			// }).Debug("client mutex lock stopSyn")

			// client.mutex.Lock()

			log.WithFields(log.Fields{
				"client": client.String(),
			}).Debug("client stopAck")

			close(client.stopAck)

			log.WithFields(log.Fields{
				"client": client.String(),
			}).Debug("client conn close")

			_ = client.conn.Close()

			log.WithFields(log.Fields{
				"client": client.String(),
			}).Debug("client reportChan close")

			close(client.reportChan)

			// log.WithFields(log.Fields{
			// 	"client": client.String(),
			// }).Debug("client mutex unlock stopSyn")

			// client.mutex.Unlock()

			return

		case <-ticker.C:
			log.WithFields(log.Fields{
				"client": client.String(),
			}).Debug("client mutex lock ticker")
			client.mutex.Lock()

			log.WithFields(log.Fields{
				"client": client.String(),
			}).Debug("client cboring WriteByteStringLen")
			err := cboring.WriteByteStringLen(0, client.conn)
			client.mutex.Unlock()
			log.WithFields(log.Fields{
				"client": client.String(),
			}).Debug("client ticker mutex unlock")

			if err != nil {
				log.WithFields(log.Fields{
					"client": client.String(),
					"error":  err,
				}).Error("MTCPClient: Keepalive errored")

				client.reportChan <- cla.NewConvergencePeerDisappeared(client, client.GetPeerEndpointID())
			}
		}
	}
}

func (client *MTCPClient) Send(bndl bpv7.Bundle) (err error) {
	defer func() {
		if r := recover(); r != nil && err == nil {
			err = fmt.Errorf("MTCPClient.Send: %v", r)
		}

		// In case of an error, report our failure upstream
		/*
			if err != nil {
				client.reportChan <- cla.NewConvergencePeerDisappeared(client, client.GetPeerEndpointID())
			}
		*/

		log.WithFields(log.Fields{
			"client": client.String(),
		}).Debug("client mutex unlock send")
		client.mutex.Unlock()
	}()

	log.WithFields(log.Fields{
		"client": client.String(),
	}).Debug("client mutex lock send")
	client.mutex.Lock()

	connWriter := bufio.NewWriter(client.conn)

	buff := new(bytes.Buffer)
	if cborErr := cboring.Marshal(&bndl, buff); cborErr != nil {
		err = cborErr
		client.reportChan <- cla.NewConvergencePeerDisappeared(client, client.GetPeerEndpointID())
		return
	}

	if bsErr := cboring.WriteByteStringLen(uint64(buff.Len()), connWriter); bsErr != nil {
		err = bsErr
		client.reportChan <- cla.NewConvergencePeerDisappeared(client, client.GetPeerEndpointID())
		return
	}

	if _, plErr := buff.WriteTo(connWriter); plErr != nil {
		err = plErr
		client.reportChan <- cla.NewConvergencePeerDisappeared(client, client.GetPeerEndpointID())
		return
	}

	if flushErr := connWriter.Flush(); flushErr != nil {
		err = flushErr
		client.reportChan <- cla.NewConvergencePeerDisappeared(client, client.GetPeerEndpointID())
		return
	}

	// Check if the connection is still alive with an empty, unbuffered packet
	if probeErr := cboring.WriteByteStringLen(0, client.conn); probeErr != nil {
		err = probeErr
		client.reportChan <- cla.NewConvergencePeerDisappeared(client, client.GetPeerEndpointID())
		return
	}

	return
}

func (client *MTCPClient) Channel() chan cla.ConvergenceStatus {
	return client.reportChan
}

func (client *MTCPClient) Close() error {
	log.WithFields(log.Fields{
		"client": client.String(),
	}).Debug("client sending stopSyn")

	close(client.stopSyn)

	log.WithFields(log.Fields{
		"client": client.String(),
	}).Debug("client waiting for stopAck")
	<-client.stopAck

	return nil
}

func (client *MTCPClient) GetPeerEndpointID() bpv7.EndpointID {
	return client.peer
}

func (client *MTCPClient) Address() string {
	return client.address
}

func (client *MTCPClient) IsPermanent() bool {
	return client.permanent
}

func (client *MTCPClient) String() string {
	if client.conn != nil {
		return fmt.Sprintf("mtcp://%v", client.conn.RemoteAddr())
	} else {
		return fmt.Sprintf("mtcp://%s", client.address)
	}
}
