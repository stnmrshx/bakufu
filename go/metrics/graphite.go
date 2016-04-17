/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package metrics

import (
	"github.com/cyberdelia/go-metrics-graphite"
	"github.com/stnmrshx/golib/log"
	"github.com/stnmrshx/bakufu/go/config"
	"github.com/stnmrshx/bakufu/go/process"
	"github.com/rcrowley/go-metrics"
	"net"
	"strings"
	"time"
)

var graphiteTickCallbacks [](func())

func InitGraphiteMetrics() error {
	if config.Config.GraphiteAddr == "" {
		return nil
	}
	if config.Config.GraphitePollSeconds <= 0 {
		return nil
	}
	if config.Config.GraphitePath == "" {
		return log.Errorf("No graphite path provided (see GraphitePath config variable). Will not log to graphite")
	}
	addr, err := net.ResolveTCPAddr("tcp", config.Config.GraphiteAddr)
	if err != nil {
		return log.Errore(err)
	}
	graphitePathHostname := process.ThisHostname
	if config.Config.GraphiteConvertHostnameDotsToUnderscores {
		graphitePathHostname = strings.Replace(graphitePathHostname, ".", "_", -1)
	}
	graphitePath := config.Config.GraphitePath
	graphitePath = strings.Replace(graphitePath, "{hostname}", graphitePathHostname, -1)

	log.Debugf("Will log to graphite on %+v, %+v", config.Config.GraphiteAddr, graphitePath)

	graphiteCallbackTick := time.Tick(time.Duration(config.Config.GraphitePollSeconds) * time.Second)
	go func() {
		go graphite.Graphite(metrics.DefaultRegistry, 1*time.Minute, graphitePath, addr)
		for range graphiteCallbackTick {
			for _, f := range graphiteTickCallbacks {
				go f()
			}
		}
	}()

	return nil
}

func OnGraphiteTick(f func()) {
	graphiteTickCallbacks = append(graphiteTickCallbacks, f)
}
