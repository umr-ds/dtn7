package core

import (
	"github.com/dtn7/dtn7-go/cla"
	log "github.com/sirupsen/logrus"
	"sync"
)

type ContextRouting struct {
	c                *Core
	contextSemaphore sync.RWMutex
}

func NewContextRouting(c *Core) *ContextRouting {
	contextRouting := ContextRouting{
		c: c,
	}
	log.Info("Initialised ContextRouting")
	return &contextRouting
}

func (contextRouting *ContextRouting) NotifyIncoming(bp BundlePack) {
	// TODO: Dummy Implementation
}

func (contextRouting *ContextRouting) DispatchingAllowed(bp BundlePack) bool {
	// TODO: Dummy Implementation
	return true
}

func (contextRouting *ContextRouting) SenderForBundle(bp BundlePack) (sender []cla.ConvergenceSender, delete bool) {
	// TODO: Dummy Implementation
	return nil, false
}

func (contextRouting *ContextRouting) ReportFailure(bp BundlePack, sender cla.ConvergenceSender) {
	// TODO: Dummy Implementation
}

func (contextRouting *ContextRouting) ReportPeerAppeared(peer cla.Convergence) {
	// TODO: Dummy Implementation
}

func (contextRouting *ContextRouting) ReportPeerDisappeared(peer cla.Convergence) {
	// TODO: Dummy Implementation
}
