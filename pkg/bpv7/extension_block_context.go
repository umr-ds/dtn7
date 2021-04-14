// SPDX-FileCopyrightText: 2019, 2020, 2021 Markus Sommer
//
// SPDX-License-Identifier: GPL-3.0-or-later

package bpv7

import (
	"encoding/json"
	"fmt"
	"github.com/dtn7/cboring"
	log "github.com/sirupsen/logrus"
	"io"
)

const (
	NodeContext   = iota
	BundleContext = iota
)

type ContextBlock struct {
	Type    uint64
	Context []byte
}

func NewNodeContextBlock(context map[string]interface{}) (*ContextBlock, error) {
	marshalled, err := json.Marshal(context)
	if err != nil {
		return nil, err
	}

	contextBlock := ContextBlock{
		Type:    NodeContext,
		Context: marshalled,
	}
	return &contextBlock, nil
}

func NewBundleContextBlock(context map[string]interface{}) (*ContextBlock, error) {
	marshalled, err := json.Marshal(context)
	if err != nil {
		return nil, err
	}

	contextBlock := ContextBlock{
		Type:    BundleContext,
		Context: marshalled,
	}
	return &contextBlock, nil
}

func (contextBlock *ContextBlock) GetContext() (map[string]interface{}, error) {
	var context map[string]interface{}
	err := json.Unmarshal(contextBlock.Context, &context)
	if err != nil {
		return nil, err
	}

	return context, nil
}

func (contextBlock *ContextBlock) BlockTypeCode() uint64 {
	return ExtBlockTypeContextBlock
}

func (contextBlock *ContextBlock) BlockTypeName() string {
	return "ContextBlock"
}

func (contextBlock *ContextBlock) CheckValid() error {
	return nil
}

func (contextBlock *ContextBlock) String() string {
	return string(contextBlock.Context)
}

func (contextBlock *ContextBlock) MarshalCbor(w io.Writer) error {
	err := cboring.WriteArrayLength(2, w)
	if err != nil {
		return err
	}

	err = cboring.WriteUInt(contextBlock.Type, w)
	if err != nil {
		return err
	}

	err = cboring.WriteByteString(contextBlock.Context, w)
	if err != nil {
		return err
	}

	return nil
}

func (contextBlock *ContextBlock) UnmarshalCbor(r io.Reader) error {
	structLength, err := cboring.ReadArrayLength(r)
	if err != nil {
		return err
	} else if structLength != 2 {
		return fmt.Errorf("expected 2 fields, got %d", structLength)
	}

	contextType, err := cboring.ReadUInt(r)
	if err != nil {
		return err
	}

	contextBlock.Type = contextType

	context, err := cboring.ReadByteString(r)
	if err != nil {
		return err
	}

	contextBlock.Context = context

	return nil
}

func (bldr *BundleBuilder) ContextBlock(args ...interface{}) *BundleBuilder {
	log.WithField("args", args).Debug("Builder args")

	if bldr.err != nil {
		return bldr
	}

	context, ok := args[0].(map[string]interface{})
	if !ok {
		bldr.err = fmt.Errorf("did not supply valid context data")
		return bldr
	}

	contextBlock, err := NewBundleContextBlock(context)
	if err != nil {
		bldr.err = err
		return bldr
	}

	return bldr.Canonical(contextBlock, DeleteBundle)
}

