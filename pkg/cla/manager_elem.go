// SPDX-FileCopyrightText: 2019, 2020 Alvar Penning
//
// SPDX-License-Identifier: GPL-3.0-or-later

package cla

import (
	"sync"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
)

// convergenceElem is a wrapper around a Convergence to assign a status,
// supervised by a Manager.
type convergenceElem struct {
	// conv is the wrapped Convergence
	conv Convergence

	// mutex protects critical parts
	mutex sync.Mutex

	// convChnl is the Manager's inChnl.
	convChnl chan ConvergenceStatus

	// ttl is used both for determining the activity and for counting-off.
	// A negative ttl implies an active convergenceElem.
	ttl int32

	// stop{Syn,Ack} are used to supervise closing this convergenceElem, see deactivate()
	stopSyn chan struct{}
	stopAck chan struct{}
}

// newConvergenceElement creates a new convergenceElem for a Convergence with
// an initial ttl value.
func newConvergenceElement(conv Convergence, convChnl chan ConvergenceStatus, ttl int32) *convergenceElem {
	return &convergenceElem{
		conv:     conv,
		convChnl: convChnl,
		ttl:      ttl,
	}
}

// asReceiver returns a ConvergenceReceiver, if one is available, as indicated
// by the boolean return value.
func (ce *convergenceElem) asReceiver() (c ConvergenceReceiver, ok bool) {
	c, ok = (ce.conv).(ConvergenceReceiver)
	return
}

// asSender returns a ConvergenceSender, if one is available, as indicated
// by the boolean return value.
func (ce *convergenceElem) asSender() (c ConvergenceSender, ok bool) {
	c, ok = (ce.conv).(ConvergenceSender)
	return
}

// isActive return if this convergenceElem is wraped around an active Convergence.
func (ce *convergenceElem) isActive() bool {
	return atomic.LoadInt32(&ce.ttl) < 0
}

// handler supervises both stopping and ConvergenceStatus forwarding to the Manager.
func (ce *convergenceElem) handler() {
	for {
		select {
		case <-ce.stopSyn:
			log.WithFields(log.Fields{
				"cla": ce.conv,
			}).Debug("Closing CLA's handler")

			if err := ce.conv.Close(); err != nil {
				log.WithField("cla", ce.conv).WithError(err).Warn("Closing CLA errored")
			}
			close(ce.stopAck)

			return

		case cs := <-ce.conv.Channel():
			log.WithFields(log.Fields{
				"cla":    ce.conv,
				"status": cs.String(),
			}).Debug("Forwarding ConvergenceStatus to Manager")

			ce.convChnl <- cs
		}
	}
}

// activate tries to start this convergenceElem. Both a success message and an
// indicator for a new attempt are returned.
func (ce *convergenceElem) activate() (successful, retry bool) {
	if ce.isActive() {
		return
	}

	ce.mutex.Lock()
	defer ce.mutex.Unlock()

	if atomic.LoadInt32(&ce.ttl) == 0 && !ce.conv.IsPermanent() {
		log.WithFields(log.Fields{
			"cla":   ce.conv,
			"error": "TTL expired",
		}).Info("Failed to start CLA")

		return false, false
	}

	claErr, claRetry := ce.conv.Start()
	if claErr == nil {
		log.WithFields(log.Fields{
			"cla": ce.conv,
		}).Info("Started CLA")

		atomic.StoreInt32(&ce.ttl, -1)

		ce.stopSyn = make(chan struct{})
		ce.stopAck = make(chan struct{})
		go ce.handler()

		return true, false
	} else {
		log.WithFields(log.Fields{
			"cla":       ce.conv,
			"permanent": ce.conv.IsPermanent(),
			"ttl":       atomic.LoadInt32(&ce.ttl),
			"retry":     claRetry,
			"error":     claErr,
		}).Info("Failed to start CLA")

		if claRetry {
			atomic.AddInt32(&ce.ttl, -1)
		} else {
			atomic.StoreInt32(&ce.ttl, 0)
		}

		return false, claRetry
	}
}

// deactivate marks this convergenceElem as deactivated. Both a new ttl as well
// as whether Stop should be executed can be specified.
func (ce *convergenceElem) deactivate(ttl int32) {
	if !ce.isActive() {
		return
	}

	ce.mutex.Lock()
	defer ce.mutex.Unlock()

	log.WithFields(log.Fields{
		"cla": ce.conv,
	}).Info("Deactivating CLA")

	close(ce.stopSyn)

	//<-ce.stopAck

	atomic.StoreInt32(&ce.ttl, ttl)
}
