package mtcp

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/dtn7/cboring"
	"github.com/dtn7/dtn7-go/bundle"
	"github.com/dtn7/dtn7-go/cla"
)

// MTCPClient is an implementation of a Minimal TCP Convergence-Layer client
// which connects to a MTCP server to send bundles. This struct implements
// a ConvergenceSender.
type MTCPClient struct {
	conn       net.Conn
	peer       bundle.EndpointID
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
func NewMTCPClient(address string, peer bundle.EndpointID, permanent bool) *MTCPClient {
	return &MTCPClient{
		peer:      peer,
		permanent: permanent,
		address:   address,
	}
}

// NewMTCPClient creates a new MTCPClient, connected to the given address. The
// permanent flag indicates if this MTCPClient should never be removed from
// the core.
func NewAnonymousMTCPClient(address string, permanent bool) *MTCPClient {
	return NewMTCPClient(address, bundle.DtnNone(), permanent)
}

func (client *MTCPClient) Start() (err error, retry bool) {
	retry = true

	conn, connErr := net.DialTimeout("tcp", client.address, time.Second)
	if connErr != nil {
		err = connErr
		return
	}

	if kaErr := setKeepAlive(conn); kaErr != nil {
		err = kaErr
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
	var ticker = time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	// Introduce ourselves once
	client.reportChan <- cla.NewConvergencePeerAppeared(client, client.GetPeerEndpointID())

	for {
		select {
		case <-client.stopSyn:
			client.mutex.Lock()
			defer client.mutex.Unlock()

			client.conn.Close()
			close(client.reportChan)

			close(client.stopAck)

			return

		case <-ticker.C:
			client.mutex.Lock()
			err := cboring.WriteByteStringLen(0, client.conn)
			client.mutex.Unlock()

			if err != nil {
				/*
				log.WithFields(log.Fields{
					"client": client.String(),
					"error":  err,
				}).Warn("MTCPClient: Keepalive errored")
				 */

				client.reportChan <- cla.NewConvergencePeerDisappeared(client, client.GetPeerEndpointID())
			}
		}
	}
}

func (client *MTCPClient) Send(bndl *bundle.Bundle) (err error) {
	defer func() {
		if r := recover(); r != nil && err == nil {
			err = fmt.Errorf("MTCPClient.Send: %v", r)
		}

		// In case of an error, report our failure upstream
		if err != nil {
			client.reportChan <- cla.NewConvergencePeerDisappeared(client, client.GetPeerEndpointID())
		}
	}()

	client.mutex.Lock()
	defer client.mutex.Unlock()

	connWriter := bufio.NewWriter(client.conn)

	buff := new(bytes.Buffer)
	if cborErr := cboring.Marshal(bndl, buff); cborErr != nil {
		err = cborErr
		return
	}

	if bsErr := cboring.WriteByteStringLen(uint64(buff.Len()), connWriter); bsErr != nil {
		err = bsErr
		return
	}

	if _, plErr := buff.WriteTo(connWriter); plErr != nil {
		err = plErr
		return
	}

	if flushErr := connWriter.Flush(); flushErr != nil {
		err = flushErr
		return
	}

	// Check if the connection is still alive with an empty, unbuffered packet
	if probeErr := cboring.WriteByteStringLen(0, client.conn); probeErr != nil {
		err = probeErr
		return
	}

	return
}

func (client *MTCPClient) Channel() chan cla.ConvergenceStatus {
	return client.reportChan
}

func (client *MTCPClient) Close() {
	close(client.stopSyn)
	<-client.stopAck
}

func (client *MTCPClient) GetPeerEndpointID() bundle.EndpointID {
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
