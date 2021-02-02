// SPDX-FileCopyrightText: 2019, 2020, 2021 Markus Sommer
//
// SPDX-License-Identifier: GPL-3.0-or-later

package bpv7

import (
	"fmt"
	"github.com/dtn7/cboring"
	"io"
)

const (
	NodeContext   = iota
	BundleContext = iota
)

type ContextBlock struct {
	Type    uint64
	Context map[string]string
}

func NewNodeContextBlock(context map[string]string) *ContextBlock {
	contextBlock := ContextBlock{
		Type:    NodeContext,
		Context: context,
	}
	return &contextBlock
}

func NewBundleContextBlock(context map[string]string) *ContextBlock {
	contextBlock := ContextBlock{
		Type:    BundleContext,
		Context: context,
	}
	return &contextBlock
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

func (contextBlock *ContextBlock) MarshalCbor(w io.Writer) error {
	err := cboring.WriteArrayLength(2, w)
	if err != nil {
		return err
	}

	err = cboring.WriteUInt(contextBlock.Type, w)
	if err != nil {
		return err
	}

	err = cboring.WriteMapPairLength(uint64(len(contextBlock.Context)), w)
	if err != nil {
		return err
	}

	for key, value := range contextBlock.Context {
		err = cboring.WriteTextString(key, w)
		if err != nil {
			return err
		}

		err = cboring.WriteTextString(value, w)
		if err != nil {
			return err
		}
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

	length, err := cboring.ReadMapPairLength(r)
	if err != nil {
		return err
	}

	context := make(map[string]string, length)
	var i uint64
	for i = 0; i < length; i++ {
		key, err := cboring.ReadTextString(r)
		if err != nil {
			return err
		}

		value, err := cboring.ReadTextString(r)
		if err != nil {
			return err
		}

		context[key] = value
	}

	contextBlock.Context = context

	return nil
}
