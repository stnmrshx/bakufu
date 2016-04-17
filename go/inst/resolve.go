/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package inst

import (
	"errors"
	"fmt"
	"github.com/stnmrshx/golib/log"
	"github.com/stnmrshx/bakufu/go/config"
	"github.com/pmylund/go-cache"
	"net"
	"regexp"
	"strings"
	"time"
)

type HostnameResolve struct {
	hostname         string
	resolvedHostname string
}

func init() {
	if config.Config.ExpiryHostnameResolvesMinutes < 1 {
		config.Config.ExpiryHostnameResolvesMinutes = 1
	}
}

var hostnameResolvesLightweightCache = cache.New(time.Duration(config.Config.ExpiryHostnameResolvesMinutes)*time.Minute, time.Minute)
var hostnameResolvesLightweightCacheLoadedOnceFromDB bool = false

func HostnameResolveMethodIsNone() bool {
	return strings.ToLower(config.Config.HostnameResolveMethod) == "none"
}

func GetCNAME(hostname string) (string, error) {
	res, err := net.LookupCNAME(hostname)
	if err != nil {
		return hostname, err
	}
	res = strings.TrimRight(res, ".")
	return res, nil
}

func resolveHostname(hostname string) (string, error) {
	switch strings.ToLower(config.Config.HostnameResolveMethod) {
	case "none":
		return hostname, nil
	case "default":
		return hostname, nil
	case "cname":
		return GetCNAME(hostname)
	}
	return hostname, nil
}

func ResolveHostname(hostname string) (string, error) {
	hostname = strings.TrimSpace(hostname)
	if hostname == "" {
		return hostname, errors.New("Will not resolve empty hostname")
	}
	if strings.Contains(hostname, ",") {
		return hostname, fmt.Errorf("Will not resolve multi-hostname: %+v", hostname)
	}
	if (&InstanceKey{Hostname: hostname}).IsDetached() {
		return hostname, fmt.Errorf("Will not resolve detached hostname: %+v", hostname)
	}

	if resolvedHostname, found := hostnameResolvesLightweightCache.Get(hostname); found {
		return resolvedHostname.(string), nil
	}

	if !hostnameResolvesLightweightCacheLoadedOnceFromDB {
		if !HostnameResolveMethodIsNone() {
			if resolvedHostname, err := ReadResolvedHostname(hostname); err == nil && resolvedHostname != "" {
				hostnameResolvesLightweightCache.Set(hostname, resolvedHostname, 0)
				return resolvedHostname, nil
			}
		}
	}

	log.Debugf("Hostname unresolved yet: %s", hostname)
	resolvedHostname, err := resolveHostname(hostname)
	if config.Config.RejectHostnameResolvePattern != "" {
		if matched, _ := regexp.MatchString(config.Config.RejectHostnameResolvePattern, resolvedHostname); matched {
			log.Warningf("ResolveHostname: %+v resolved to %+v but rejected due to RejectHostnameResolvePattern '%+v'", hostname, resolvedHostname, config.Config.RejectHostnameResolvePattern)
			return hostname, nil
		}
	}

	if err != nil {
		hostnameResolvesLightweightCache.Set(hostname, resolvedHostname, time.Minute)
		return hostname, err
	}
	log.Debugf("Cache hostname resolve %s as %s", hostname, resolvedHostname)
	UpdateResolvedHostname(hostname, resolvedHostname)
	return resolvedHostname, nil
}

func UpdateResolvedHostname(hostname string, resolvedHostname string) bool {
	if resolvedHostname == "" {
		return false
	}
	if existingResolvedHostname, found := hostnameResolvesLightweightCache.Get(hostname); found && (existingResolvedHostname == resolvedHostname) {
		return false
	}
	hostnameResolvesLightweightCache.Set(hostname, resolvedHostname, 0)
	if !HostnameResolveMethodIsNone() {
		WriteResolvedHostname(hostname, resolvedHostname)
	}
	return true
}

func LoadHostnameResolveCache() error {
	if !HostnameResolveMethodIsNone() {
		return loadHostnameResolveCacheFromDatabase()
	}
	return nil
}

func loadHostnameResolveCacheFromDatabase() error {
	allHostnamesResolves, err := readAllHostnameResolves()
	if err != nil {
		return err
	}
	for _, hostnameResolve := range allHostnamesResolves {
		hostnameResolvesLightweightCache.Set(hostnameResolve.hostname, hostnameResolve.resolvedHostname, 0)
	}
	hostnameResolvesLightweightCacheLoadedOnceFromDB = true
	return nil
}

func FlushNontrivialResolveCacheToDatabase() error {
	if !HostnameResolveMethodIsNone() {
		return log.Errorf("FlushNontrivialResolveCacheToDatabase() called, but HostnameResolveMethod is %+v", config.Config.HostnameResolveMethod)
	}
	items, _ := HostnameResolveCache()
	for hostname := range items {
		resolvedHostname, found := hostnameResolvesLightweightCache.Get(hostname)
		if found && (resolvedHostname.(string) != hostname) {
			WriteResolvedHostname(hostname, resolvedHostname.(string))
		}
	}
	return nil
}

func ResetHostnameResolveCache() error {
	err := deleteHostnameResolves()
	hostnameResolvesLightweightCache.Flush()
	hostnameResolvesLightweightCacheLoadedOnceFromDB = false
	return err
}

func HostnameResolveCache() (map[string]*cache.Item, error) {
	return hostnameResolvesLightweightCache.Items(), nil
}

func UnresolveHostname(instanceKey *InstanceKey) (InstanceKey, bool, error) {
	if *config.RuntimeCLIFlags.SkipUnresolve {
		return *instanceKey, false, nil
	}
	unresolvedHostname, err := readUnresolvedHostname(instanceKey.Hostname)
	if err != nil {
		return *instanceKey, false, log.Errore(err)
	}
	if unresolvedHostname == instanceKey.Hostname {
		return *instanceKey, false, nil
	}
	unresolvedKey := &InstanceKey{Hostname: unresolvedHostname, Port: instanceKey.Port}

	instance, err := ReadTopologyInstance(unresolvedKey)
	if err != nil {
		return *instanceKey, false, log.Errore(err)
	}
	if instance.IsBinlogServer() && config.Config.SkipBinlogServerUnresolveCheck {
	} else if instance.Key.Hostname != instanceKey.Hostname {
		if *config.RuntimeCLIFlags.SkipUnresolveCheck {
			return *instanceKey, false, nil
		}
		return *instanceKey, false, log.Errorf("Error unresolving; hostname=%s, unresolved=%s, re-resolved=%s; mismatch. Skip/ignore with --skip-unresolve-check", instanceKey.Hostname, unresolvedKey.Hostname, instance.Key.Hostname)
	}
	return *unresolvedKey, true, nil
}

func RegisterHostnameUnresolve(instanceKey *InstanceKey, unresolvedHostname string) (err error) {
	return WriteHostnameUnresolve(instanceKey, unresolvedHostname)
}
