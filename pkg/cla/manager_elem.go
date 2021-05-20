// SPDX-FileCopyrightText: 2019, 2020 Alvar Penning
//
// SPDX-License-Identifier: GPL-3.0-or-later

package cla

import (
	"sync"

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
	ttl int

	// stop{Syn,Ack} are used to supervise closing this convergenceElem, see deactivate()
	stopSyn chan struct{}
	stopAck chan struct{}
}

// newConvergenceElement creates a new convergenceElem for a Convergence with
// an initial ttl value.
func newConvergenceElement(conv Convergence, convChnl chan ConvergenceStatus, ttl int) *convergenceElem {
	return &convergenceElem{
		conv:     conv,
		convChnl: convChnl,
		ttl:      ttl,
	}
}

// asReceiver returns a ConvergenceReceiver, if one is available, as indicated
// by the boolean return value.
func (ce *convergenceElem) asReceiver() (c ConvergenceReceiver, ok bool) {
	log.WithField("Receiver", ce.conv.Address()).Debug("asReceiver")
	c, ok = (ce.conv).(ConvergenceReceiver)
	return
}

// asSender returns a ConvergenceSender, if one is available, as indicated
// by the boolean return value.
func (ce *convergenceElem) asSender() (c ConvergenceSender, ok bool) {
	c, ok = (ce.conv).(ConvergenceSender)
	return
}

// isActive return if this convergenceElem is wrapped around an active Convergence.
func (ce *convergenceElem) isActive() bool {
	//log.WithField("receiver", ce.conv.Address()).Debug("Entering isActive Mutex")
	ce.mutex.Lock()
	defer func() {
		ce.mutex.Unlock()
		//log.WithField("receiver", ce.conv.Address()).Debug("Leaving isActive Mutex")
	}()

	log.WithField("receiver", ce.conv.Address()).Debug("Checking activity")
	return ce.ttl < 0
}

// handler supervises both stopping and ConvergenceStatus forwarding to the Manager.
func (ce *convergenceElem) handler() {
	for {
		select {
		case <-ce.stopSyn:
			log.WithFields(log.Fields{
				"cla": ce.conv,
			}).Debug("Closing CLA's handler")

			log.WithFields(log.Fields{
				"cla": ce.conv,
			}).Debug("Sending stopAck")
			close(ce.stopAck)
			log.WithFields(log.Fields{
				"cla": ce.conv,
			}).Debug("Sent stopAck")

			if err := ce.conv.Close(); err != nil {
				log.WithField("cla", ce.conv).WithError(err).Warn("Closing CLA errored")
			}

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

	log.WithField("receiver", ce.conv.Address()).Debug("Entering activate Mutex")
	ce.mutex.Lock()
	defer func() {
		ce.mutex.Unlock()
		log.WithField("receiver", ce.conv.Address()).Debug("Leaving activate Mutex")
	}()

	if ce.ttl == 0 && !ce.conv.IsPermanent() {
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

		ce.ttl = -1

		ce.stopSyn = make(chan struct{})
		ce.stopAck = make(chan struct{})
		go ce.handler()

		return true, false
	} else {
		log.WithFields(log.Fields{
			"cla":       ce.conv,
			"permanent": ce.conv.IsPermanent(),
			"ttl":       ce.ttl,
			"retry":     claRetry,
			"error":     claErr,
		}).Info("Failed to start CLA")

		if claRetry {
			ce.ttl -= 1
		} else {
			ce.ttl = 0
		}

		return false, claRetry
	}
}

// deactivate marks this convergenceElem as deactivated. Both a new ttl as well
// as whether Stop should be executed can be specified.
func (ce *convergenceElem) deactivate(ttl int) {
	if !ce.isActive() {
		return
	}

	log.WithField("receiver", ce.conv.Address()).Debug("Entering deactivate Mutex")
	ce.mutex.Lock()
	defer func() {
		ce.mutex.Unlock()
		log.WithField("receiver", ce.conv.Address()).Debug("Leaving deactivate Mutex")
	}()

	log.WithFields(log.Fields{
		"cla": ce.conv,
	}).Info("Deactivating CLA")

	close(ce.stopSyn)

	log.WithFields(log.Fields{
		"cla": ce.conv,
	}).Info("Waiting for stopAck")
	<-ce.stopAck

	log.WithFields(log.Fields{
		"cla": ce.conv,
	}).Info("Got stopAck")

	ce.ttl = ttl
}
