/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package logic

import (
	"github.com/stnmrshx/bakufu/go/inst"
)

type AsyncRequest struct {
	Id                  int64
	Story               string
	Command             string
	OperatedInstanceKey *inst.InstanceKey
	DestinationKey      *inst.InstanceKey
	Pattern             string
	GTIDHint            inst.OperationGTIDHint
}

func NewEmptyAsyncRequest() *AsyncRequest {
	asyncRequest := &AsyncRequest{}
	asyncRequest.GTIDHint = inst.GTIDHintNeutral
	return asyncRequest
}

func NewAsyncRequest(story string, command string, instanceKey *inst.InstanceKey, destinationKey *inst.InstanceKey, pattern string, gtidHint inst.OperationGTIDHint) *AsyncRequest {
	asyncRequest := NewEmptyAsyncRequest()
	asyncRequest.Story = story
	asyncRequest.Command = command
	asyncRequest.OperatedInstanceKey = instanceKey
	asyncRequest.DestinationKey = destinationKey
	asyncRequest.Pattern = pattern
	asyncRequest.GTIDHint = gtidHint
	return asyncRequest
}

func NewSimpleAsyncRequest(story string, command string, instanceKey *inst.InstanceKey) *AsyncRequest {
	asyncRequest := NewEmptyAsyncRequest()
	asyncRequest.Story = story
	asyncRequest.Command = command
	asyncRequest.OperatedInstanceKey = instanceKey
	asyncRequest.DestinationKey = nil
	asyncRequest.Pattern = ""
	return asyncRequest
}
