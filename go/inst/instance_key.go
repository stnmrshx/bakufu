/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package inst

import (
	"fmt"
	"github.com/stnmrshx/bakufu/go/config"
	"strconv"
	"strings"
)

type InstanceKey struct {
	Hostname string
	Port     int
}

const detachHint = "//"

func NewRawInstanceKey(hostPort string) (*InstanceKey, error) {
	tokens := strings.SplitN(hostPort, ":", 2)
	if len(tokens) != 2 {
		return nil, fmt.Errorf("Cannot parse InstanceKey from %s. Expected format is host:port", hostPort)
	}
	instanceKey := &InstanceKey{Hostname: tokens[0]}
	var err error
	if instanceKey.Port, err = strconv.Atoi(tokens[1]); err != nil {
		return instanceKey, fmt.Errorf("Invalid port: %s", tokens[1])
	}

	return instanceKey, nil
}

func NewInstanceKeyFromStrings(hostname string, port string) (*InstanceKey, error) {
	instanceKey := &InstanceKey{}
	var err error
	if instanceKey.Port, err = strconv.Atoi(port); err != nil {
		return instanceKey, fmt.Errorf("Invalid port: %s", port)
	}

	if instanceKey.Hostname, err = ResolveHostname(hostname); err != nil {
		return instanceKey, err
	}

	return instanceKey, nil
}

func ParseInstanceKey(hostPort string) (*InstanceKey, error) {
	tokens := strings.SplitN(hostPort, ":", 2)
	if len(tokens) != 2 {
		return nil, fmt.Errorf("Cannot parse InstanceKey from %s. Expected format is host:port", hostPort)
	}
	return NewInstanceKeyFromStrings(tokens[0], tokens[1])
}

func ParseInstanceKeyLoose(hostPort string) (*InstanceKey, error) {
	if !strings.Contains(hostPort, ":") {
		return &InstanceKey{Hostname: hostPort, Port: config.Config.DefaultInstancePort}, nil
	}
	return ParseInstanceKey(hostPort)
}

func (this *InstanceKey) Formalize() *InstanceKey {
	this.Hostname, _ = ResolveHostname(this.Hostname)
	return this
}

func (this *InstanceKey) Equals(other *InstanceKey) bool {
	if other == nil {
		return false
	}
	return this.Hostname == other.Hostname && this.Port == other.Port
}

func (this *InstanceKey) SmallerThan(other *InstanceKey) bool {
	if this.Hostname < other.Hostname {
		return true
	}
	if this.Hostname == other.Hostname && this.Port < other.Port {
		return true
	}
	return false
}

func (this *InstanceKey) IsDetached() bool {
	return strings.HasPrefix(this.Hostname, detachHint)
}

func (this *InstanceKey) IsValid() bool {
	if this.Hostname == "_" {
		return false
	}
	if this.IsDetached() {
		return false
	}
	return len(this.Hostname) > 0 && this.Port > 0
}

func (this *InstanceKey) DetachedKey() *InstanceKey {
	if this.IsDetached() {
		return this
	}
	return &InstanceKey{Hostname: fmt.Sprintf("%s%s", detachHint, this.Hostname), Port: this.Port}
}

func (this *InstanceKey) ReattachedKey() *InstanceKey {
	if !this.IsDetached() {
		return this
	}
	return &InstanceKey{Hostname: this.Hostname[len(detachHint):], Port: this.Port}
}

func (this *InstanceKey) StringCode() string {
	return fmt.Sprintf("%s:%d", this.Hostname, this.Port)
}

func (this *InstanceKey) DisplayString() string {
	return this.StringCode()
}

func (this InstanceKey) String() string {
	return this.StringCode()
}
