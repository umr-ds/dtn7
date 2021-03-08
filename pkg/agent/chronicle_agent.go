// SPDX-FileCopyrightText: 2021 Markus Sommer
//
// SPDX-License-Identifier: GPL-3.0-or-later

package agent

import (
	"github.com/dtn7/dtn7-go/pkg/bpv7"
	log "github.com/sirupsen/logrus"
)

const chronicleEndpoint = "dtn://chronicle/"

type ChronicleAgent struct {
	endpoint             bpv7.EndpointID
	chronicleRestAddress string

	receiver chan Message
	sender   chan Message
}

func NewChronicleAgent(chronicleRestAddress string) *ChronicleAgent {
	endpoint, err := bpv7.NewEndpointID(chronicleEndpoint)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
			"chronicleRestAddress": chronicleEndpoint,
		}).Fatal("Address did not compile.")
	}

	agent := &ChronicleAgent{
		endpoint:             endpoint,
		chronicleRestAddress: chronicleRestAddress,
		receiver:             make(chan Message),
		sender:               make(chan Message),
	}

	go agent.handle()

	return agent
}

func (agent *ChronicleAgent) handle() {
	defer close(agent.sender)
	for msg := range agent.receiver {
		switch msg := msg.(type) {
		case BundleMessage:
			agent.receivedBundle(msg.Bundle)

		case ShutdownMessage:
			log.Debug("ChronicleDB Agent is shutting down")
			return

		default:
			log.WithField("message", msg).Info("ChronicleDB Agent received unknown / unsupported message")
		}
	}
}

func (agent *ChronicleAgent) receivedBundle(bundle bpv7.Bundle) {

}

func (agent *ChronicleAgent) Endpoints() []bpv7.EndpointID {
	return []bpv7.EndpointID{agent.endpoint}
}

func (agent *ChronicleAgent) MessageReceiver() chan Message {
	return agent.receiver
}

func (agent *ChronicleAgent) MessageSender() chan Message {
	return agent.sender
}
