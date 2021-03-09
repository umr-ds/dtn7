// SPDX-FileCopyrightText: 2021 Markus Sommer
//
// SPDX-License-Identifier: GPL-3.0-or-later

package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/dtn7/dtn7-go/pkg/bpv7"
	log "github.com/sirupsen/logrus"
)

const chronicleEndpoint = "dtn://chronicle/"

type EventInsert struct {
	StreamName string                   `json:"streamName"`
	Events     []map[string]interface{} `json:"events"`
}

type StreamCreation struct {
	StreamName string       `json:"streamName"`
	Schema     []SchemaItem `json:"schema"`
}

type SchemaItem struct {
	Name       string          `json:"name"`
	Type       string          `json:"type"`
	Properties map[string]bool `json:"properties"`
}

type ChronicleAgent struct {
	endpoint               bpv7.EndpointID
	chronicleRestAddress   string
	streamCreationEndpoint string
	eventInsertionEndpoint string

	receiver chan Message
	sender   chan Message
}

func NewChronicleAgent(chronicleRestAddress string) *ChronicleAgent {
	endpoint, err := bpv7.NewEndpointID(chronicleEndpoint)
	if err != nil {
		log.WithFields(log.Fields{
			"error":                err,
			"chronicleRestAddress": chronicleEndpoint,
		}).Fatal("Address did not compile.")
	}

	agent := &ChronicleAgent{
		endpoint:               endpoint,
		chronicleRestAddress:   chronicleRestAddress,
		streamCreationEndpoint: fmt.Sprintf("http://%v/native/create-stream", chronicleRestAddress),
		eventInsertionEndpoint: fmt.Sprintf("http://%v/native/insert", chronicleRestAddress),
		receiver:               make(chan Message),
		sender:                 make(chan Message),
	}

	go agent.handle()

	return agent
}

func (agent *ChronicleAgent) handle() {
	defer close(agent.sender)

	log.Info("Start ChronicleDB Application Agent")

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
	if metaDataBlock, err := bundle.ExtensionBlock(bpv7.ExtBlockTypeChronicleBlock); err == nil {
		chronicleBlock := metaDataBlock.Value.(*bpv7.ChronicleBlock)
		if chronicleBlock.Operation == bpv7.ChronicleCreateStream {
			pb, err := bundle.PayloadBlock()
			if err != nil {
				log.WithFields(log.Fields{
					"error":  err,
					"bundle": bundle.ID(),
				}).Error("Bundle contained no schema definition")
				return
			}
			payloadBlock := pb.Value.(*bpv7.PayloadBlock)

			if err := agent.CreateStream(chronicleBlock.StreamName, payloadBlock); err != nil {
				log.WithFields(log.Fields{
					"error":  err,
					"bundle": bundle.ID(),
				}).Error("Error creating ChronicleDB stream")
				return
			} else {
				log.Debug("ChronicleDB stream Creation successful")
			}
		} else if chronicleBlock.Operation == bpv7.ChronicleInsertEvents {
			pb, err := bundle.PayloadBlock()
			if err != nil {
				log.WithFields(log.Fields{
					"error":  err,
					"bundle": bundle.ID(),
				}).Error("Bundle contained no events to insert")
				return
			}
			payloadBlock := pb.Value.(*bpv7.PayloadBlock)
			err = agent.InsertEvents(chronicleBlock.StreamName, payloadBlock)
			if err != nil {
				log.WithFields(log.Fields{
					"error":  err,
					"bundle": bundle.ID(),
				}).Error("Error inserting ChronicleDB events.")
			} else {
				log.Debug("ChronicleDB event insertion successful.")
			}
		}
	} else {
		log.WithField("bundle", bundle.ID()).Error("Bundle addressed to ChronicleDB did not contain necessary metadata")
	}
}

func (agent *ChronicleAgent) CreateStream(streamName string, payload *bpv7.PayloadBlock) error {
	var schema []SchemaItem
	err := json.Unmarshal(payload.Data(), &schema)
	if err != nil {
		return err
	}

	stream := StreamCreation{
		StreamName: streamName,
		Schema:     schema,
	}
	log.WithField("stream", stream).Debug("Stream creation")

	queryString, err := json.Marshal(stream)
	if err != nil {
		return err
	}

	_, err = http.Post(agent.streamCreationEndpoint, "application/json", bytes.NewReader(queryString))
	if err != nil {
		return err
	}

	return nil
}

func (agent *ChronicleAgent) InsertEvents(streamName string, payload *bpv7.PayloadBlock) error {
	var events []map[string]interface{}
	err := json.Unmarshal(payload.Data(), &events)

	insert := EventInsert{
		StreamName: streamName,
		Events:     events,
	}
	log.WithField("data", insert).Debug("Event insertion")

	queryString, err := json.Marshal(insert)
	if err != nil {
		return err
	}

	_, err = http.Post(agent.eventInsertionEndpoint, "application/json", bytes.NewReader(queryString))
	if err != nil {
		return err
	}

	return nil
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
