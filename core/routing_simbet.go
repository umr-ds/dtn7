package core

import (
	"github.com/dtn7/dtn7-go/bundle"
	"github.com/dtn7/dtn7-go/cla"
	log "github.com/sirupsen/logrus"
	"sync"
)

type SimBet struct {
	c *Core
	// dataMutex is a RW-mutex which protects change operations to the algorithm's metadata
	dataMutex sync.RWMutex
	// adjacencies is the adjacency-list for nodes connected to this one
	// uses map instead of slice to ensure uniqueness of elements
	adjacencies map[bundle.EndpointID]bool
}

func NewSimBet(c *Core) *SimBet {
	log.Debug("Initialised SimBet routing")
	simBet := SimBet{
		c:           c,
		adjacencies: make(map[bundle.EndpointID]bool),
	}
	return &simBet
}

func (simBet *SimBet) NotifyIncoming(bp BundlePack) {
	// TODO: Dummy Implementation
}

func (simBet *SimBet) DispatchingAllowed(bp BundlePack) bool {
	// TODO: Dummy Implementation
	return true
}

func (simBet *SimBet) SenderForBundle(bp BundlePack) (sender []cla.ConvergenceSender, delete bool) {
	// TODO: Dummy Implementation
	return nil, false
}

func (simBet *SimBet) ReportFailure(bp BundlePack, sender cla.ConvergenceSender) {
	// TODO: Dummy Implementation
}

func (simBet *SimBet) ReportPeerAppeared(peer cla.Convergence) {
	log.WithFields(log.Fields{
		"address": peer,
	}).Debug("Peer appeared")

	peerReceiver, ok := peer.(cla.ConvergenceSender)
	if !ok {
		log.Debug("Peer was not a ConvergenceSender")
		return
	}

	peerID := peerReceiver.GetPeerEndpointID()

	log.WithFields(log.Fields{
		"peer": peerID,
	}).Debug("PeerID discovered")

	simBet.dataMutex.Lock()
	defer simBet.dataMutex.Unlock()

	simBet.adjacencies[peerID] = true

	log.WithFields(log.Fields{
		"peer": peerID,
	}).Debug("Peer added to adjacencies")
}

func (simBet *SimBet) ReportPeerDisappeared(peer cla.Convergence) {
	log.WithFields(log.Fields{
		"address": peer,
	}).Debug("Peer disappeared")

	peerReceiver, ok := peer.(cla.ConvergenceSender)
	if !ok {
		log.Debug("Peer was not a ConvergenceSender")
		return
	}

	peerID := peerReceiver.GetPeerEndpointID()

	log.WithFields(log.Fields{
		"peer": peerID,
	}).Debug("PeerID discovered")

	simBet.dataMutex.Lock()
	defer simBet.dataMutex.Unlock()

	delete(simBet.adjacencies, peerID)

	log.WithFields(log.Fields{
		"peer": peerID,
	}).Debug("Peer removed from adjacencies")
}
