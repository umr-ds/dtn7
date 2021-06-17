// SPDX-FileCopyrightText: 2021 Markus Sommer
//
// SPDX-License-Identifier: GPL-3.0-or-later

package quicl

import (
	"context"
	"errors"
	"time"

	"github.com/dtn7/dtn7-go/pkg/bpv7"
	"github.com/dtn7/dtn7-go/pkg/cla"
	"github.com/dtn7/dtn7-go/pkg/cla/quicl/internal"
	"github.com/lucas-clemente/quic-go"
	log "github.com/sirupsen/logrus"
)

type Listener struct {
	listenAddress string
	endpointID    bpv7.EndpointID
	manager       *cla.Manager

	stop chan struct{}
}

func NewQUICListener(listenAddress string, endpointID bpv7.EndpointID) *Listener {
	return &Listener{
		listenAddress: listenAddress,
		endpointID:    endpointID,
		manager:       nil,
		stop:          make(chan struct{}),
	}
}

/**
Methods for Convergable interface
*/

func (listener *Listener) Close() error {
	close(listener.stop)
	return nil
}

/**
Methods for ConvergenceProvider interface
*/

func (listener *Listener) RegisterManager(manager *cla.Manager) {
	listener.manager = manager
}

func (listener *Listener) Start() error {
	log.WithField("address", listener.listenAddress).Info("Starting QUICL-listener")
	lst, err := quic.ListenAddr(listener.listenAddress, internal.GenerateListenerTLSConfig(), internal.GenerateQUICConfig())
	if err != nil {
		log.WithError(err).Error("Error creating QUICL listener")
		return err
	}

	go listener.handle(lst)

	return nil
}

/*
Non-interface methods
*/

func (listener *Listener) handle(lst quic.Listener) {
	log.WithField("address", listener.listenAddress).Info("Listening for QUICL connections")
	timeout := 50 * time.Millisecond

	for {
		select {
		case <-listener.stop:
			log.WithField("address", listener.listenAddress).Info("Shutting down QUICL listener")
			err := lst.Close()
			if err != nil {
				log.WithFields(log.Fields{
					"address": listener.listenAddress,
					"error":   err,
				}).Error("Error closing QUIC listener")
			}
			return

		default:
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			session, err := lst.Accept(ctx)
			cancel()
			if err != nil {
				if !(errors.Is(err, context.DeadlineExceeded)) {
					log.WithFields(log.Fields{
						"address": listener.listenAddress,
						"error":   err,
					}).Error("Error accepting QUIC connection")
				}
			} else {
				log.WithFields(log.Fields{
					"address": listener.listenAddress,
					"peer":    session.RemoteAddr(),
				}).Info("QUICL listener accepted new connection")
				endpoint := NewListenerEndpoint(listener.endpointID, session)
				go listener.manager.Register(endpoint)
			}
		}
	}
}
