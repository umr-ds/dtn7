package core

import (
	"github.com/dtn7/dtn7-go/bundle"
	"github.com/dtn7/dtn7-go/cla"
	"github.com/dtn7/dtn7-go/storage"
	log "github.com/sirupsen/logrus"
)

// RoutingAlgorithm is an interface to specify routing algorithms for
// delay-tolerant networks. An implementation might store a reference to a Core
// struct to refer the ConvergenceSenders.
type RoutingAlgorithm interface {
	// NotifyIncoming notifies this RoutingAlgorithm about incoming bundles.
	// Whether the algorithm acts on this information or ignores it, is both a
	// design and implementation decision.
	NotifyIncoming(bp BundlePack)

	// DispatchingAllowed will be called from within the *dispatching* step of
	// the processing pipeline. A RoutingAlgorithm is allowed to drop the
	// proceeding of a bundle before being inspected further or being delivered
	// locally or to another node.
	DispatchingAllowed(bp BundlePack) bool

	// SenderForBundle returns an array of ConvergenceSender for a requested
	// bundle. Furthermore the finished flags indicates if this BundlePack should
	// be deleted afterwards.
	// The CLA selection is based on the algorithm's design.
	SenderForBundle(bp BundlePack) (sender []cla.ConvergenceSender, delete bool)

	// ReportFailure notifies the RoutingAlgorithm about a failed transmission to
	// a previously selected CLA. Compare: SenderForBundle.
	ReportFailure(bp BundlePack, sender cla.ConvergenceSender)

	// ReportPeerAppeared notifies the RoutingAlgorithm about a new neighbor.
	ReportPeerAppeared(peer cla.Convergence)

	// ReportPeerDisappeared notifies the RoutingAlgorithm about the
	// disappearance of a neighbor.
	ReportPeerDisappeared(peer cla.Convergence)
}

// RoutingConfig contains necessary configuration data to initialise a routing algorithm
type RoutingConf struct {
	// Algorithm is one of the implemented routing-algorithms
	// May be: "epidemic", "spray", "binary_spray", "dtlsr"
	Algorithm string
	// SprayConf contains data to initialise spray & binary_spray
	SprayConf SprayConfig
	// DTLSRConf contains data to initialise dtlsr
	DTLSRConf DTLSRConfig
	// ProphetConf contains data to initialise prophet
	ProphetConf ProphetConfig
	// ContextConf contains data to initialise context routing
	ContextConf ContextConfig
}

// sendMetadataBundle can be used by routing algorithm to send relevant metadata to peers
// Metadata needs to be serialised as an ExtensionBlock
func sendMetadataBundle(c *Core, source bundle.EndpointID, destination bundle.EndpointID, metadataBlock bundle.ExtensionBlock) error {
	bundleBuilder := bundle.Builder()
	bundleBuilder.Source(source)
	bundleBuilder.Destination(destination)
	bundleBuilder.CreationTimestampNow()
	bundleBuilder.Lifetime("1m")
	bundleBuilder.BundleCtrlFlags(bundle.MustNotFragmented)
	// no Payload
	bundleBuilder.PayloadBlock(byte(1))

	bundleBuilder.Canonical(metadataBlock)
	metadataBundle, err := bundleBuilder.Build()
	if err != nil {
		return err
	} else {
		log.Debug("Metadata Bundle built")
	}

	log.Debug("Sending metadata bundle")
	c.SendBundle(&metadataBundle)
	log.WithFields(log.Fields{
		"bundle": metadataBundle,
	}).Debug("Successfully sent metadata bundle")

	return nil
}

// filterCLAs filters the node's which already received a Bundle for a specific routing algorithm, e.g., "epidemic".
func filterCLAs(bundleItem storage.BundleItem, clas []cla.ConvergenceSender, algorithm string) (filtered []cla.ConvergenceSender, sentEids []bundle.EndpointID) {
	filtered = make([]cla.ConvergenceSender, 0)

	sentEids, ok := bundleItem.Properties["routing/"+algorithm+"/sent"].([]bundle.EndpointID)
	if !ok {
		sentEids = make([]bundle.EndpointID, 0)
	}

	for _, cs := range clas {
		skip := false

		for _, eid := range sentEids {
			if cs.GetPeerEndpointID() == eid {
				skip = true
				break
			}
		}

		if !skip {
			filtered = append(filtered, cs)
			sentEids = append(sentEids, cs.GetPeerEndpointID())
		}
	}

	return
}
