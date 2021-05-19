// SPDX-FileCopyrightText: 2019 Markus Sommer
// SPDX-FileCopyrightText: 2019, 2020 Alvar Penning
//
// SPDX-License-Identifier: GPL-3.0-or-later

package routing

import (
	log "github.com/sirupsen/logrus"

	"github.com/dtn7/dtn7-go/pkg/bpv7"
	"github.com/dtn7/dtn7-go/pkg/cla"
)

// EpidemicRouting is an implementation of a Algorithm and behaves in a
// flooding-based epidemic way.
type EpidemicRouting struct {
	c *Core
}

// NewEpidemicRouting creates a new EpidemicRouting Algorithm interacting
// with the given Core.
func NewEpidemicRouting(c *Core) *EpidemicRouting {
	log.Debug("Initialised epidemic routing")

	return &EpidemicRouting{c: c}
}

// NotifyNewBundle tells the EpidemicRouting about new bundles.
//
// In our case, the PreviousNodeBlock will be inspected.
func (er *EpidemicRouting) NotifyNewBundle(bp BundleDescriptor) {
	log.WithFields(log.Fields{
		"bundle": bp.ID(),
	}).Debug("Incoming bundle")

	bi, biErr := er.c.store.QueryId(bp.Id)
	if biErr != nil {
		log.WithFields(log.Fields{
			"error": biErr,
		}).Warn("Failed to proceed a non-stored Bundle")
		return
	}

	bndl := bp.MustBundle()

	if _, ok := bi.Properties["routing/epidemic/destination"]; !ok {
		bi.Properties["routing/epidemic/destination"] = bndl.PrimaryBlock.Destination
		if err := er.c.store.Update(bi); err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Warn("Updating BundleItem failed")
		}
	}

	// Check if we got a PreviousNodeBlock and extract its EndpointID
	var prevNode bpv7.EndpointID
	if pnBlock, err := bndl.ExtensionBlock(bpv7.ExtBlockTypePreviousNodeBlock); err == nil {
		prevNode = pnBlock.Value.(*bpv7.PreviousNodeBlock).Endpoint()

		log.WithFields(log.Fields{
			"bundle": bndl.ID(),
			"src":    prevNode,
		}).Info("Received bundle from peer")
	} else {
		return
	}

	sentEids, ok := bi.Properties["routing/epidemic/sent"].([]bpv7.EndpointID)
	if !ok {
		sentEids = make([]bpv7.EndpointID, 0)
	}

	// Check if PreviousNodeBlock is already known
	for _, eids := range sentEids {
		if eids == prevNode {
			return
		}
	}

	log.WithFields(log.Fields{
		"bundle": bp.ID(),
		"eid":    prevNode,
	}).Debug("EpidemicRouting received an incoming bundle and checked its PreviousNodeBlock")

	bi.Properties["routing/epidemic/sent"] = append(sentEids, prevNode)
	if err := er.c.store.Update(bi); err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Warn("Updating BundleItem failed")
	}
}

func (er *EpidemicRouting) clasForBundle(bp BundleDescriptor, updateDb bool) (css []cla.ConvergenceSender, del bool) {
	bi, biErr := er.c.store.QueryId(bp.Id)
	if biErr != nil {
		log.WithFields(log.Fields{
			"bundle": bp.ID(),
			"error":  biErr,
		}).Warn("Failed to proceed a non-stored Bundle")
		return nil, false
	}

	css, sentEids := filterCLAs(bi, er.c.claManager.Sender(), "epidemic")

	log.WithFields(log.Fields{
		"bundle": bp.ID(),
		"sent":   sentEids,
	}).Debug("EpidemicRouting is processing an outgoing bundle")

	if updateDb {
		bi.Properties["routing/epidemic/sent"] = sentEids
		if err := er.c.store.Update(bi); err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Warn("Updating BundleItem failed")
		}
	}

	log.WithFields(log.Fields{
		"bundle":              bp.ID(),
		"sent":                sentEids,
		"convergence-senders": css,
	}).Debug("EpidemicRouting selected Convergence Senders for an outbounding bundle")

	del = false
	return
}

// DispatchingAllowed only allows dispatching, iff the bundle is addressed to
// this Node or if any known CLA without having received this bundle exists.
func (er *EpidemicRouting) DispatchingAllowed(bp BundleDescriptor) bool {
	log.WithField("bundle", bp.ID()).Debug("Querying store")
	bi, biErr := er.c.store.QueryId(bp.Id)
	log.WithField("bundle", bp.ID()).Debug("Queried store")
	if biErr != nil {
		log.WithFields(log.Fields{
			"bundle": bp.ID(),
			"error":  biErr,
		}).Warn("Failed to proceed a non-stored Bundle")

		return true
	} else if dst, ok := bi.Properties["routing/epidemic/destination"]; ok {
		log.WithField("bundle", bp.ID()).Debug("Got destination")
		if er.c.HasEndpoint(dst.(bpv7.EndpointID)) {
			log.WithField("bundle", bp.ID()).Debug("Bundle is for us")
			return true
		}
	}

	log.WithField("bundle", bp.ID()).Debug("Computing CLAs for bundle")
	css, _ := er.clasForBundle(bp, false)
	log.WithField("bundle", bp.ID()).Debug("CLAs computed")

	if len(css) == 0 {
		log.WithField("bundle", bp.ID()).Debug("List of CLAs empty")
		bi.Pending = true
		if err := er.c.store.Update(bi); err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Warn("Updating BundleItem failed")
		}
		log.WithField("bundle", bp.ID()).Debug("Updated store")
	}

	return len(css) > 0
}

// SenderForBundle returns the Core's ConvergenceSenders.
func (er *EpidemicRouting) SenderForBundle(bp BundleDescriptor) (css []cla.ConvergenceSender, del bool) {
	log.WithFields(log.Fields{
		"bundle": bp.ID(),
	}).Debug("Starting routing decision")

	defer log.WithFields(log.Fields{
		"bundle": bp.ID(),
	}).Debug("Routing decision finished")

	return er.clasForBundle(bp, true)
}

func (er *EpidemicRouting) ReportFailure(bp BundleDescriptor, sender cla.ConvergenceSender) {
	bi, biErr := er.c.store.QueryId(bp.Id)
	if biErr != nil {
		log.WithFields(log.Fields{
			"error": biErr,
		}).Warn("Failed to proceed a non-stored Bundle")
		return
	}

	sentEids, ok := bi.Properties["routing/epidemic/sent"].([]bpv7.EndpointID)
	if !ok {
		sentEids = make([]bpv7.EndpointID, 0)
	}

	log.WithFields(log.Fields{
		"bundle":  bp.ID(),
		"bad_cla": sender,
		"sent":    sentEids,
	}).Debug("EpidemicRouting failed to transmit to CLA")

	for i := 0; i < len(sentEids); i++ {
		if sentEids[i] == sender.GetPeerEndpointID() {
			sentEids = append(sentEids[:i], sentEids[i+1:]...)
			break
		}
	}

	bi.Properties["routing/epidemic/sent"] = sentEids
	if err := er.c.store.Update(bi); err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Warn("Updating BundleItem failed")
	}
}

func (_ *EpidemicRouting) ReportPeerAppeared(_ cla.Convergence) {}

func (_ *EpidemicRouting) ReportPeerDisappeared(_ cla.Convergence) {}

func (_ *EpidemicRouting) String() string {
	return "epidemic"
}
