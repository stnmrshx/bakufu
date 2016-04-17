/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package logic

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/stnmrshx/golib/log"
	"github.com/stnmrshx/golib/math"
	"github.com/stnmrshx/bakufu/go/agent"
	"github.com/stnmrshx/bakufu/go/config"
	"github.com/stnmrshx/bakufu/go/inst"
	ometrics "github.com/stnmrshx/bakufu/go/metrics"
	"github.com/stnmrshx/bakufu/go/process"
	"github.com/pmylund/go-cache"
	"github.com/rcrowley/go-metrics"
)

const (
	maxConcurrency = 5
)

var discoveryInstanceKeys = make(chan inst.InstanceKey, maxConcurrency)
var discoveriesCounter = metrics.NewCounter()
var failedDiscoveriesCounter = metrics.NewCounter()
var discoveryQueueLengthGauge = metrics.NewGauge()
var discoveryRecentCountGauge = metrics.NewGauge()
var isElectedGauge = metrics.NewGauge()

var isElectedNode = false

var recentDiscoveryOperationKeys *cache.Cache

func init() {
	isElectedNode = false

	metrics.Register("discoveries.attempt", discoveriesCounter)
	metrics.Register("discoveries.fail", failedDiscoveriesCounter)
	metrics.Register("discoveries.queue_length", discoveryQueueLengthGauge)
	metrics.Register("discoveries.recent_count", discoveryRecentCountGauge)
	metrics.Register("elect.is_elected", isElectedGauge)

	ometrics.OnGraphiteTick(func() { discoveryQueueLengthGauge.Update(int64(len(discoveryInstanceKeys))) })
	ometrics.OnGraphiteTick(func() {
		if recentDiscoveryOperationKeys == nil {
			return
		}
		discoveryRecentCountGauge.Update(int64(recentDiscoveryOperationKeys.ItemCount()))
	})
	ometrics.OnGraphiteTick(func() { isElectedGauge.Update(int64(math.TernaryInt(isElectedNode, 1, 0))) })
}

func acceptSignals() {
	c := make(chan os.Signal, 1)

	signal.Notify(c, syscall.SIGHUP)
	go func() {
		for sig := range c {
			switch sig {
			case syscall.SIGHUP:
				log.Debugf("Received SIGHUP. Reloading configuration")
				config.Reload()
				inst.AuditOperation("reload-configuration", nil, "Triggered via SIGHUP")
			}
		}
	}()
}

func handleDiscoveryRequests() {
	for instanceKey := range discoveryInstanceKeys {
		if isElectedNode {
			go discoverInstance(instanceKey)
		} else {
			log.Debugf("Node apparently demoted. Skipping discovery of %+v. Remaining queue size: %+v", instanceKey, len(discoveryInstanceKeys))
		}
	}
}

func discoverInstance(instanceKey inst.InstanceKey) {
	instanceKey.Formalize()
	if !instanceKey.IsValid() {
		return
	}

	if existsInCacheError := recentDiscoveryOperationKeys.Add(instanceKey.DisplayString(), true, cache.DefaultExpiration); existsInCacheError != nil {
		return
	}

	instance, found, err := inst.ReadInstance(&instanceKey)

	if found && instance.IsUpToDate && instance.IsLastCheckValid {
		return
	}
	discoveriesCounter.Inc(1)
	instance, err = inst.ReadTopologyInstance(&instanceKey)
	if instance == nil {
		failedDiscoveriesCounter.Inc(1)
		log.Warningf("instance is nil in discoverInstance. key=%+v, error=%+v", instanceKey, err)
		return
	}

	log.Debugf("Discovered host: %+v, master: %+v, version: %+v", instance.Key, instance.MasterKey, instance.Version)

	if !isElectedNode {
		return
	}

	for _, slaveKey := range instance.SlaveHosts.GetInstanceKeys() {
		slaveKey := slaveKey
		if slaveKey.IsValid() {
			discoveryInstanceKeys <- slaveKey
		}
	}
	if instance.MasterKey.IsValid() {
		discoveryInstanceKeys <- instance.MasterKey
	}
}

func ContinuousDiscovery() {
	if config.Config.DatabaselessMode__experimental {
		log.Fatal("Cannot execute continuous mode in databaseless mode")
	}

	log.Infof("Starting continuous discovery")
	recentDiscoveryOperationKeys = cache.New(time.Duration(config.Config.DiscoveryPollSeconds)*time.Second, time.Second)

	inst.LoadHostnameResolveCache()
	go handleDiscoveryRequests()

	discoveryTick := time.Tick(time.Duration(config.Config.DiscoveryPollSeconds) * time.Second)
	instancePollTick := time.Tick(time.Duration(config.Config.InstancePollSeconds) * time.Second)
	caretakingTick := time.Tick(time.Minute)
	recoveryTick := time.Tick(time.Duration(config.Config.RecoveryPollSeconds) * time.Second)
	var snapshotTopologiesTick <-chan time.Time
	if config.Config.SnapshotTopologiesIntervalHours > 0 {
		snapshotTopologiesTick = time.Tick(time.Duration(config.Config.SnapshotTopologiesIntervalHours) * time.Hour)
	}

	go ometrics.InitGraphiteMetrics()
	go acceptSignals()

	if *config.RuntimeCLIFlags.GrabElection {
		process.GrabElection()
	}
	for {
		select {
		case <-discoveryTick:
			go func() {
				wasAlreadyElected := isElectedNode
				if isElectedNode, _ = process.AttemptElection(); isElectedNode {
					instanceKeys, _ := inst.ReadOutdatedInstanceKeys()
					log.Debugf("outdated keys: %+v", instanceKeys)
					for _, instanceKey := range instanceKeys {
						instanceKey := instanceKey

						go func() {
							if instanceKey.IsValid() {
								discoveryInstanceKeys <- instanceKey
							}
						}()
					}
					if !wasAlreadyElected {
						// Just turned to be leader!
						go process.RegisterNode("", "", false)
					}
				} else {
					log.Debugf("Not elected as active node; polling")
				}
			}()
		case <-instancePollTick:
			go func() {
				if isElectedNode {
					go inst.UpdateInstanceRecentRelaylogHistory()
					go inst.RecordInstanceCoordinatesHistory()
				}
			}()
		case <-caretakingTick:
			go func() {
				if isElectedNode {
					go inst.RecordInstanceBinlogFileHistory()
					go inst.ForgetLongUnseenInstances()
					go inst.ForgetUnseenInstancesDifferentlyResolved()
					go inst.ForgetExpiredHostnameResolves()
					go inst.DeleteInvalidHostnameResolves()
					go inst.ReviewUnseenInstances()
					go inst.InjectUnseenMasters()
					go inst.ResolveUnknownMasterHostnameResolves()
					go inst.UpdateClusterAliases()
					go inst.ExpireMaintenance()
					go inst.ExpireDowntime()
					go inst.ExpireCandidateInstances()
					go inst.ExpireHostnameUnresolve()
					go inst.ExpireClusterDomainName()
					go inst.ExpireAudit()
					go inst.ExpireMasterPositionEquivalence()
					go inst.FlushNontrivialResolveCacheToDatabase()
					go process.ExpireNodesHistory()
					go process.ExpireAccessTokens()
				}
				if !isElectedNode {
					go inst.LoadHostnameResolveCache()
				}
				go inst.ReadClusterAliases()
			}()
		case <-recoveryTick:
			go func() {
				if isElectedNode {
					go ClearActiveFailureDetections()
					go ClearActiveRecoveries()
					go ExpireBlockedRecoveries()
					go AcknowledgeCrashedRecoveries()
					go inst.ExpireInstanceAnalysisChangelog()
					go CheckAndRecover(nil, nil, false)
				}
			}()
		case <-snapshotTopologiesTick:
			go func() {
				go inst.SnapshotTopologies()
			}()
		}
	}
}

func pollAgent(hostname string) error {
	polledAgent, err := agent.GetAgent(hostname)
	agent.UpdateAgentLastChecked(hostname)

	if err != nil {
		return log.Errore(err)
	}

	err = agent.UpdateAgentInfo(hostname, polledAgent)
	if err != nil {
		return log.Errore(err)
	}

	return nil
}

func ContinuousAgentsPoll() {
	log.Infof("Starting continuous agents poll")

	go discoverSeededAgents()

	tick := time.Tick(time.Duration(config.Config.DiscoveryPollSeconds) * time.Second)
	caretakingTick := time.Tick(time.Hour)
	for range tick {
		agentsHosts, _ := agent.ReadOutdatedAgentsHosts()
		log.Debugf("outdated agents hosts: %+v", agentsHosts)
		for _, hostname := range agentsHosts {
			go pollAgent(hostname)
		}
		select {
		case <-caretakingTick:
			agent.ForgetLongUnseenAgents()
			agent.FailStaleSeeds()
		default:
		}
	}
}

func discoverSeededAgents() {
	for seededAgent := range agent.SeededAgents {
		instanceKey := &inst.InstanceKey{Hostname: seededAgent.Hostname, Port: int(seededAgent.MySQLPort)}
		go inst.ReadTopologyInstance(instanceKey)
	}
}
