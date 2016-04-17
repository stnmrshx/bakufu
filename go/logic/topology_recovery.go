/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package logic

import (
	"fmt"
	"github.com/stnmrshx/golib/log"
	"github.com/stnmrshx/bakufu/go/config"
	"github.com/stnmrshx/bakufu/go/inst"
	"github.com/stnmrshx/bakufu/go/os"
	"github.com/stnmrshx/bakufu/go/process"
	"github.com/pmylund/go-cache"
	"github.com/rcrowley/go-metrics"
	"sort"
	"strings"
	"time"
)

type BlockedTopologyRecovery struct {
	FailedInstanceKey    inst.InstanceKey
	ClusterName          string
	Analysis             inst.AnalysisCode
	LastBlockedTimestamp string
	BlockingRecoveryId   int64
}

type TopologyRecovery struct {
	inst.PostponedFunctionsContainer

	Id                        int64
	AnalysisEntry             inst.ReplicationAnalysis
	SuccessorKey              *inst.InstanceKey
	IsActive                  bool
	IsSuccessful              bool
	LostSlaves                inst.InstanceKeyMap
	ParticipatingInstanceKeys inst.InstanceKeyMap
	AllErrors                 []string
	RecoveryStartTimestamp    string
	RecoveryEndTimestamp      string
	ProcessingNodeHostname    string
	ProcessingNodeToken       string
	PostponedFunctions        [](func() error)
	Acknowledged              bool
	AcknowledgedAt            string
	AcknowledgedBy            string
	AcknowledgedComment       string
	LastDetectionId           int64
	RelatedRecoveryId         int64
}

func NewTopologyRecovery(replicationAnalysis inst.ReplicationAnalysis) *TopologyRecovery {
	topologyRecovery := &TopologyRecovery{}
	topologyRecovery.AnalysisEntry = replicationAnalysis
	topologyRecovery.SuccessorKey = nil
	topologyRecovery.LostSlaves = *inst.NewInstanceKeyMap()
	topologyRecovery.ParticipatingInstanceKeys = *inst.NewInstanceKeyMap()
	topologyRecovery.AllErrors = []string{}
	topologyRecovery.PostponedFunctions = [](func() error){}
	return topologyRecovery
}

func (this *TopologyRecovery) AddError(err error) error {
	if err != nil {
		this.AllErrors = append(this.AllErrors, err.Error())
	}
	return err
}

func (this *TopologyRecovery) AddErrors(errs []error) {
	for _, err := range errs {
		this.AddError(err)
	}
}

type MasterRecoveryType string

const (
	MasterRecoveryGTID         MasterRecoveryType = "MasterRecoveryGTID"
	MasterRecoveryPseudoGTID                      = "MasterRecoveryPseudoGTID"
	MasterRecoveryBinlogServer                    = "MasterRecoveryBinlogServer"
)

var emptySlavesList [](*inst.Instance)

var emergencyReadTopologyInstanceMap = cache.New(time.Duration(config.Config.DiscoveryPollSeconds)*time.Second, time.Duration(config.Config.DiscoveryPollSeconds)*time.Second)

type InstancesByCountSlaves [](*inst.Instance)

func (this InstancesByCountSlaves) Len() int      { return len(this) }
func (this InstancesByCountSlaves) Swap(i, j int) { this[i], this[j] = this[j], this[i] }
func (this InstancesByCountSlaves) Less(i, j int) bool {
	if len(this[i].SlaveHosts) == len(this[j].SlaveHosts) {
		return !this[i].ExecBinlogCoordinates.SmallerThan(&this[j].ExecBinlogCoordinates)
	}
	return len(this[i].SlaveHosts) < len(this[j].SlaveHosts)
}

var recoverDeadMasterCounter = metrics.NewCounter()
var recoverDeadMasterSuccessCounter = metrics.NewCounter()
var recoverDeadMasterFailureCounter = metrics.NewCounter()
var recoverDeadIntermediateMasterCounter = metrics.NewCounter()
var recoverDeadIntermediateMasterSuccessCounter = metrics.NewCounter()
var recoverDeadIntermediateMasterFailureCounter = metrics.NewCounter()
var recoverDeadCoMasterCounter = metrics.NewCounter()
var recoverDeadCoMasterSuccessCounter = metrics.NewCounter()
var recoverDeadCoMasterFailureCounter = metrics.NewCounter()

func init() {
	metrics.Register("recover.dead_master.start", recoverDeadMasterCounter)
	metrics.Register("recover.dead_master.success", recoverDeadMasterSuccessCounter)
	metrics.Register("recover.dead_master.fail", recoverDeadMasterFailureCounter)
	metrics.Register("recover.dead_intermediate_master.start", recoverDeadIntermediateMasterCounter)
	metrics.Register("recover.dead_intermediate_master.success", recoverDeadIntermediateMasterSuccessCounter)
	metrics.Register("recover.dead_intermediate_master.fail", recoverDeadIntermediateMasterFailureCounter)
	metrics.Register("recover.dead_co_master.start", recoverDeadCoMasterCounter)
	metrics.Register("recover.dead_co_master.success", recoverDeadCoMasterSuccessCounter)
	metrics.Register("recover.dead_co_master.fail", recoverDeadCoMasterFailureCounter)
}

func replaceCommandPlaceholders(command string, topologyRecovery *TopologyRecovery) string {
	analysisEntry := &topologyRecovery.AnalysisEntry
	command = strings.Replace(command, "{failureType}", string(analysisEntry.Analysis), -1)
	command = strings.Replace(command, "{failureDescription}", analysisEntry.Description, -1)
	command = strings.Replace(command, "{failedHost}", analysisEntry.AnalyzedInstanceKey.Hostname, -1)
	command = strings.Replace(command, "{failedPort}", fmt.Sprintf("%d", analysisEntry.AnalyzedInstanceKey.Port), -1)
	command = strings.Replace(command, "{failureCluster}", analysisEntry.ClusterDetails.ClusterName, -1)
	command = strings.Replace(command, "{failureClusterAlias}", analysisEntry.ClusterDetails.ClusterAlias, -1)
	command = strings.Replace(command, "{countSlaves}", fmt.Sprintf("%d", analysisEntry.CountSlaves), -1)
	command = strings.Replace(command, "{isDowntimed}", fmt.Sprint(analysisEntry.IsDowntimed), -1)
	command = strings.Replace(command, "{autoMasterRecovery}", fmt.Sprint(analysisEntry.ClusterDetails.HasAutomatedMasterRecovery), -1)
	command = strings.Replace(command, "{autoIntermediateMasterRecovery}", fmt.Sprint(analysisEntry.ClusterDetails.HasAutomatedIntermediateMasterRecovery), -1)
	command = strings.Replace(command, "{bakufuHost}", process.ThisHostname, -1)

	command = strings.Replace(command, "{isSuccessful}", fmt.Sprint(topologyRecovery.SuccessorKey != nil), -1)
	if topologyRecovery.SuccessorKey != nil {
		command = strings.Replace(command, "{successorHost}", topologyRecovery.SuccessorKey.Hostname, -1)
		command = strings.Replace(command, "{successorPort}", fmt.Sprintf("%d", topologyRecovery.SuccessorKey.Port), -1)
	}

	command = strings.Replace(command, "{lostSlaves}", topologyRecovery.LostSlaves.ToCommaDelimitedList(), -1)
	command = strings.Replace(command, "{slaveHosts}", analysisEntry.SlaveHosts.ToCommaDelimitedList(), -1)

	return command
}

func executeProcesses(processes []string, description string, topologyRecovery *TopologyRecovery, failOnError bool) error {
	var err error
	for _, command := range processes {
		command := replaceCommandPlaceholders(command, topologyRecovery)

		if cmdErr := os.CommandRun(command); cmdErr == nil {
			log.Infof("Executed %s command: %s", description, command)
		} else {
			if err == nil {
				err = cmdErr
			}
			log.Errorf("Failed to execute %s command: %s", description, command)
			if failOnError {
				return err
			}
		}
	}
	return err
}

func recoverDeadMasterInBinlogServerTopology(topologyRecovery *TopologyRecovery) (promotedSlave *inst.Instance, err error) {
	failedMasterKey := &topologyRecovery.AnalysisEntry.AnalyzedInstanceKey

	var promotedBinlogServer *inst.Instance

	_, promotedBinlogServer, err = inst.RegroupSlavesBinlogServers(failedMasterKey, true)
	if err != nil {
		return nil, log.Errore(err)
	}
	promotedBinlogServer, err = inst.StopSlave(&promotedBinlogServer.Key)
	if err != nil {
		return promotedSlave, log.Errore(err)
	}
	promotedSlave, err = inst.GetCandidateSlaveOfBinlogServerTopology(&promotedBinlogServer.Key)
	if err != nil {
		return promotedSlave, log.Errore(err)
	}
	promotedSlave, err = inst.StopSlave(&promotedSlave.Key)
	if err != nil {
		return promotedSlave, log.Errore(err)
	}
	promotedSlave, err = inst.StartSlaveUntilMasterCoordinates(&promotedSlave.Key, &promotedBinlogServer.ExecBinlogCoordinates)
	if err != nil {
		return promotedSlave, log.Errore(err)
	}
	promotedSlave, err = inst.StopSlave(&promotedSlave.Key)
	if err != nil {
		return promotedSlave, log.Errore(err)
	}
	promotedSlave, err = inst.ResetSlave(&promotedSlave.Key)
	if err != nil {
		return promotedSlave, log.Errore(err)
	}
	promotedSlave, err = inst.FlushBinaryLogsTo(&promotedSlave.Key, promotedBinlogServer.ExecBinlogCoordinates.LogFile)
	if err != nil {
		return promotedSlave, log.Errore(err)
	}
	promotedSlave, err = inst.FlushBinaryLogs(&promotedSlave.Key, 1)
	if err != nil {
		return promotedSlave, log.Errore(err)
	}
	promotedSlave, err = inst.PurgeBinaryLogsToCurrent(&promotedSlave.Key)
	if err != nil {
		return promotedSlave, log.Errore(err)
	}
	promotedBinlogServer, err = inst.SkipToNextBinaryLog(&promotedBinlogServer.Key)
	if err != nil {
		return promotedSlave, log.Errore(err)
	}
	promotedBinlogServer, err = inst.Repoint(&promotedBinlogServer.Key, &promotedSlave.Key, inst.GTIDHintDeny)
	if err != nil {
		return nil, log.Errore(err)
	}

	func() {
		binlogServerSlaves, err := inst.ReadBinlogServerSlaveInstances(&promotedBinlogServer.Key)
		if err != nil {
			return
		}
		maxBinlogServersToPromote := 3
		for i, binlogServerSlave := range binlogServerSlaves {
			binlogServerSlave := binlogServerSlave
			if i >= maxBinlogServersToPromote {
				return
			}
			postponedFunction := func() error {
				binlogServerSlave, err := inst.StopSlave(&binlogServerSlave.Key)
				if err != nil {
					return err
				}
				if binlogServerSlave.ExecBinlogCoordinates.SmallerThan(&promotedBinlogServer.ExecBinlogCoordinates) {
					binlogServerSlave, err = inst.StartSlaveUntilMasterCoordinates(&binlogServerSlave.Key, &promotedBinlogServer.ExecBinlogCoordinates)
					if err != nil {
						return err
					}
				}
				_, err = inst.Repoint(&binlogServerSlave.Key, &promotedSlave.Key, inst.GTIDHintDeny)
				return err
			}
			topologyRecovery.AddPostponedFunction(postponedFunction)
		}
	}()

	return promotedSlave, err
}

func RecoverDeadMaster(topologyRecovery *TopologyRecovery, skipProcesses bool) (promotedSlave *inst.Instance, lostSlaves [](*inst.Instance), err error) {
	analysisEntry := &topologyRecovery.AnalysisEntry
	failedInstanceKey := &analysisEntry.AnalyzedInstanceKey

	inst.AuditOperation("recover-dead-master", failedInstanceKey, "problem found; will recover")
	if !skipProcesses {
		if err := executeProcesses(config.Config.PreFailoverProcesses, "PreFailoverProcesses", topologyRecovery, true); err != nil {
			return nil, lostSlaves, topologyRecovery.AddError(err)
		}
	}

	log.Debugf("topology_recovery: RecoverDeadMaster: will recover %+v", *failedInstanceKey)

	var masterRecoveryType MasterRecoveryType = MasterRecoveryPseudoGTID
	if analysisEntry.OracleGTIDImmediateTopology || analysisEntry.MariaDBGTIDImmediateTopology {
		masterRecoveryType = MasterRecoveryGTID
	} else if analysisEntry.BinlogServerImmediateTopology {
		masterRecoveryType = MasterRecoveryBinlogServer
	}
	log.Debugf("topology_recovery: RecoverDeadMaster: masterRecoveryType=%+v", masterRecoveryType)

	switch masterRecoveryType {
	case MasterRecoveryGTID:
		{
			lostSlaves, _, promotedSlave, err = inst.RegroupSlavesGTID(failedInstanceKey, true, nil)
		}
	case MasterRecoveryPseudoGTID:
		{
			lostSlaves, _, _, promotedSlave, err = inst.RegroupSlavesPseudoGTIDIncludingSubSlavesOfBinlogServers(failedInstanceKey, true, nil, &topologyRecovery.PostponedFunctionsContainer)
		}
	case MasterRecoveryBinlogServer:
		{
			promotedSlave, err = recoverDeadMasterInBinlogServerTopology(topologyRecovery)
		}
	}
	topologyRecovery.AddError(err)

	if promotedSlave != nil && len(lostSlaves) > 0 && config.Config.DetachLostSlavesAfterMasterFailover {
		postponedFunction := func() error {
			log.Debugf("topology_recovery: - RecoverDeadMaster: lost %+v slaves during recovery process; detaching them", len(lostSlaves))
			for _, slave := range lostSlaves {
				slave := slave
				inst.DetachSlaveOperation(&slave.Key)
			}
			return nil
		}
		topologyRecovery.AddPostponedFunction(postponedFunction)
	}
	if config.Config.MasterFailoverLostInstancesDowntimeMinutes > 0 {
		postponedFunction := func() error {
			inst.BeginDowntime(failedInstanceKey, inst.GetMaintenanceOwner(), "RecoverDeadMaster indicates this instance is lost", config.Config.MasterFailoverLostInstancesDowntimeMinutes*60)
			for _, slave := range lostSlaves {
				slave := slave
				inst.BeginDowntime(&slave.Key, inst.GetMaintenanceOwner(), "RecoverDeadMaster indicates this instance is lost", config.Config.MasterFailoverLostInstancesDowntimeMinutes*60)
			}
			return nil
		}
		topologyRecovery.AddPostponedFunction(postponedFunction)
	}

	if promotedSlave == nil {
		inst.AuditOperation("recover-dead-master", failedInstanceKey, "Failure: no slave promoted.")
	} else {
		inst.AuditOperation("recover-dead-master", failedInstanceKey, fmt.Sprintf("promoted slave: %+v", promotedSlave.Key))
	}
	return promotedSlave, lostSlaves, err
}

func replacePromotedSlaveWithCandidate(deadInstanceKey *inst.InstanceKey, promotedSlave *inst.Instance, candidateInstanceKey *inst.InstanceKey) (*inst.Instance, error) {
	candidateSlaves, _ := inst.ReadClusterCandidateInstances(promotedSlave.ClusterName)
	log.Infof("topology_recovery: checking if should replace promoted slave with a better candidate")
	if candidateInstanceKey == nil {
		if deadInstance, _, err := inst.ReadInstance(deadInstanceKey); err == nil && deadInstance != nil {
			for _, candidateSlave := range candidateSlaves {
				if promotedSlave.Key.Equals(&candidateSlave.Key) &&
					promotedSlave.DataCenter == deadInstance.DataCenter &&
					promotedSlave.PhysicalEnvironment == deadInstance.PhysicalEnvironment {
					log.Infof("topology_recovery: promoted slave %+v is the ideal candidate", promotedSlave.Key)
					return promotedSlave, nil
				}
			}
		}
	}
	if candidateInstanceKey == nil {
		if deadInstance, _, err := inst.ReadInstance(deadInstanceKey); err == nil && deadInstance != nil {
			for _, candidateSlave := range candidateSlaves {
				if candidateSlave.DataCenter == deadInstance.DataCenter &&
					candidateSlave.PhysicalEnvironment == deadInstance.PhysicalEnvironment &&
					candidateSlave.MasterKey.Equals(&promotedSlave.Key) {
					candidateInstanceKey = &candidateSlave.Key
					log.Debugf("topology_recovery: no candidate was offered for %+v but bakufu picks %+v as candidate replacement, based on being in same DC & env as failed instance", promotedSlave.Key, candidateSlave.Key)
				}
			}
		}
	}
	if candidateInstanceKey == nil {
		for _, candidateSlave := range candidateSlaves {
			if promotedSlave.Key.Equals(&candidateSlave.Key) {
				log.Infof("topology_recovery: promoted slave %+v is a good candidate", promotedSlave.Key)
				return promotedSlave, nil
			}
		}
	}
	if candidateInstanceKey == nil {
		for _, candidateSlave := range candidateSlaves {
			if promotedSlave.DataCenter == candidateSlave.DataCenter &&
				promotedSlave.PhysicalEnvironment == candidateSlave.PhysicalEnvironment &&
				candidateSlave.MasterKey.Equals(&promotedSlave.Key) {
				candidateInstanceKey = &candidateSlave.Key
				log.Debugf("topology_recovery: no candidate was offered for %+v but bakufu picks %+v as candidate replacement, based on being in same DC & env as promoted instance", promotedSlave.Key, candidateSlave.Key)
			}
		}
	}

	if candidateInstanceKey == nil {
		return promotedSlave, nil
	}
	if promotedSlave.Key.Equals(candidateInstanceKey) {
		return promotedSlave, nil
	}

	log.Debugf("topology_recovery: promoted instance %+v is not the suggested candidate %+v. Will see what can be done", promotedSlave.Key, *candidateInstanceKey)

	candidateInstance, _, err := inst.ReadInstance(candidateInstanceKey)
	if err != nil {
		return promotedSlave, log.Errore(err)
	}

	if candidateInstance.MasterKey.Equals(&promotedSlave.Key) {
		log.Debugf("topology_recovery: suggested candidate %+v is slave of promoted instance %+v. Will try and enslave its master", *candidateInstanceKey, promotedSlave.Key)
		candidateInstance, err = inst.EnslaveMaster(&candidateInstance.Key)
		if err != nil {
			return promotedSlave, log.Errore(err)
		}
		log.Debugf("topology_recovery: success promoting %+v over %+v", *candidateInstanceKey, promotedSlave.Key)
		return candidateInstance, nil
	}

	log.Debugf("topology_recovery: could not manage to promoted suggested candidate %+v", *candidateInstanceKey)
	return promotedSlave, nil
}

func checkAndRecoverDeadMaster(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *TopologyRecovery, error) {
	if !(forceInstanceRecovery || analysisEntry.ClusterDetails.HasAutomatedMasterRecovery) {
		return false, nil, nil
	}
	topologyRecovery, err := AttemptRecoveryRegistration(&analysisEntry, !forceInstanceRecovery, !forceInstanceRecovery)
	if topologyRecovery == nil {
		log.Debugf("topology_recovery: found an active or recent recovery on %+v. Will not issue another RecoverDeadMaster.", analysisEntry.AnalyzedInstanceKey)
		return false, nil, err
	}

	log.Debugf("topology_recovery: will handle DeadMaster event on %+v", analysisEntry.ClusterDetails.ClusterName)
	recoverDeadMasterCounter.Inc(1)
	promotedSlave, lostSlaves, err := RecoverDeadMaster(topologyRecovery, skipProcesses)
	topologyRecovery.LostSlaves.AddInstances(lostSlaves)

	if promotedSlave != nil {
		promotedSlave, err = replacePromotedSlaveWithCandidate(&analysisEntry.AnalyzedInstanceKey, promotedSlave, candidateInstanceKey)
		topologyRecovery.AddError(err)
	}
	ResolveRecovery(topologyRecovery, promotedSlave)
	if promotedSlave != nil {
		recoverDeadMasterSuccessCounter.Inc(1)

		if config.Config.ApplyMySQLPromotionAfterMasterFailover {
			log.Debugf("topology_recovery: - RecoverDeadMaster: will apply MySQL changes to promoted master")
			inst.ResetSlaveOperation(&promotedSlave.Key)
			inst.SetReadOnly(&promotedSlave.Key, false)
		}
		if !skipProcesses {
			executeProcesses(config.Config.PostMasterFailoverProcesses, "PostMasterFailoverProcesses", topologyRecovery, false)
		}
	} else {
		recoverDeadMasterFailureCounter.Inc(1)
	}

	return true, topologyRecovery, err
}

func isGeneralyValidAsCandidateSiblingOfIntermediateMaster(sibling *inst.Instance) bool {
	if !sibling.LogBinEnabled {
		return false
	}
	if !sibling.LogSlaveUpdatesEnabled {
		return false
	}
	if !sibling.SlaveRunning() {
		return false
	}
	if !sibling.IsLastCheckValid {
		return false
	}
	return true
}

func isValidAsCandidateSiblingOfIntermediateMaster(intermediateMasterInstance *inst.Instance, sibling *inst.Instance) bool {
	if sibling.Key.Equals(&intermediateMasterInstance.Key) {
		return false
	}
	if !isGeneralyValidAsCandidateSiblingOfIntermediateMaster(sibling) {
		return false
	}
	if sibling.HasReplicationFilters != intermediateMasterInstance.HasReplicationFilters {
		return false
	}
	if sibling.IsBinlogServer() != intermediateMasterInstance.IsBinlogServer() {
		return false
	}
	if sibling.ExecBinlogCoordinates.SmallerThan(&intermediateMasterInstance.ExecBinlogCoordinates) {
		return false
	}
	return true
}

func GetCandidateSiblingOfIntermediateMaster(intermediateMasterInstance *inst.Instance) (*inst.Instance, error) {

	siblings, err := inst.ReadSlaveInstances(&intermediateMasterInstance.MasterKey)
	if err != nil {
		return nil, err
	}
	if len(siblings) <= 1 {
		return nil, log.Errorf("topology_recovery: no siblings found for %+v", intermediateMasterInstance.Key)
	}

	sort.Sort(sort.Reverse(InstancesByCountSlaves(siblings)))

	log.Infof("topology_recovery: searching for the best candidate sibling of dead intermediate master")
	for _, sibling := range siblings {
		sibling := sibling
		if isValidAsCandidateSiblingOfIntermediateMaster(intermediateMasterInstance, sibling) &&
			sibling.IsCandidate &&
			sibling.DataCenter == intermediateMasterInstance.DataCenter &&
			sibling.PhysicalEnvironment == intermediateMasterInstance.PhysicalEnvironment {
			log.Infof("topology_recovery: found %+v as the ideal candidate", sibling.Key)
			return sibling, nil
		}
	}
	for _, sibling := range siblings {
		sibling := sibling
		if isValidAsCandidateSiblingOfIntermediateMaster(intermediateMasterInstance, sibling) &&
			sibling.DataCenter == intermediateMasterInstance.DataCenter &&
			sibling.PhysicalEnvironment == intermediateMasterInstance.PhysicalEnvironment {
			log.Infof("topology_recovery: found %+v as a replacement in same dc & environment", sibling.Key)
			return sibling, nil
		}
	}
	for _, sibling := range siblings {
		sibling := sibling
		if isValidAsCandidateSiblingOfIntermediateMaster(intermediateMasterInstance, sibling) && sibling.IsCandidate {
			log.Infof("topology_recovery: found %+v as a good candidate", sibling.Key)
			return sibling, nil
		}
	}
	for _, sibling := range siblings {
		sibling := sibling
		if isValidAsCandidateSiblingOfIntermediateMaster(intermediateMasterInstance, sibling) {
			log.Infof("topology_recovery: found %+v as a replacement", sibling.Key)
			return sibling, nil
		}
	}
	return nil, log.Errorf("topology_recovery: cannot find candidate sibling of %+v", intermediateMasterInstance.Key)
}

func RecoverDeadIntermediateMaster(topologyRecovery *TopologyRecovery, skipProcesses bool) (successorInstance *inst.Instance, err error) {
	analysisEntry := &topologyRecovery.AnalysisEntry
	failedInstanceKey := &analysisEntry.AnalyzedInstanceKey
	recoveryResolved := false

	inst.AuditOperation("recover-dead-intermediate-master", failedInstanceKey, "problem found; will recover")
	if !skipProcesses {
		if err := executeProcesses(config.Config.PreFailoverProcesses, "PreFailoverProcesses", topologyRecovery, true); err != nil {
			return nil, topologyRecovery.AddError(err)
		}
	}

	intermediateMasterInstance, _, err := inst.ReadInstance(failedInstanceKey)
	if err != nil {
		return nil, topologyRecovery.AddError(err)
	}
	candidateSiblingOfIntermediateMaster, err := GetCandidateSiblingOfIntermediateMaster(intermediateMasterInstance)
	relocateSlavesToCandidateSibling := func() {
		if candidateSiblingOfIntermediateMaster == nil {
			return
		}
		log.Debugf("topology_recovery: - RecoverDeadIntermediateMaster: will attempt a candidate intermediate master: %+v", candidateSiblingOfIntermediateMaster.Key)
		relocatedSlaves, candidateSibling, err, errs := inst.RelocateSlaves(failedInstanceKey, &candidateSiblingOfIntermediateMaster.Key, "")
		topologyRecovery.AddErrors(errs)
		topologyRecovery.ParticipatingInstanceKeys.AddKey(candidateSiblingOfIntermediateMaster.Key)

		if len(relocatedSlaves) == 0 {
			log.Debugf("topology_recovery: - RecoverDeadIntermediateMaster: failed to move any slave to candidate intermediate master (%+v)", candidateSibling.Key)
			return
		}
		if err != nil || len(errs) > 0 {
			log.Debugf("topology_recovery: - RecoverDeadIntermediateMaster: move to candidate intermediate master (%+v) did not complete: %+v", candidateSibling.Key, err)
			return
		}
		if err == nil {
			recoveryResolved = true
			successorInstance = candidateSibling

			inst.AuditOperation("recover-dead-intermediate-master", failedInstanceKey, fmt.Sprintf("Relocated %d slaves under candidate sibling: %+v; %d errors: %+v", len(relocatedSlaves), candidateSibling.Key, len(errs), errs))
		}
	}
	if candidateSiblingOfIntermediateMaster != nil && candidateSiblingOfIntermediateMaster.DataCenter == intermediateMasterInstance.DataCenter {
		relocateSlavesToCandidateSibling()
	}
	if !recoveryResolved {
		log.Debugf("topology_recovery: - RecoverDeadIntermediateMaster: will next attempt regrouping of slaves")
		_, _, _, regroupPromotedSlave, err := inst.RegroupSlaves(failedInstanceKey, true, nil, nil)
		if err != nil {
			topologyRecovery.AddError(err)
			log.Debugf("topology_recovery: - RecoverDeadIntermediateMaster: regroup failed on: %+v", err)
		}
		if regroupPromotedSlave != nil {
			topologyRecovery.ParticipatingInstanceKeys.AddKey(regroupPromotedSlave.Key)
		}
		if candidateSiblingOfIntermediateMaster != nil && candidateSiblingOfIntermediateMaster.DataCenter != intermediateMasterInstance.DataCenter {
			log.Debugf("topology_recovery: - RecoverDeadIntermediateMaster: will next attempt relocating to another DC server")
			relocateSlavesToCandidateSibling()
		}
	}
	if !recoveryResolved {
		log.Debugf("topology_recovery: - RecoverDeadIntermediateMaster: will next attempt to relocate up from %+v", *failedInstanceKey)

		var errs []error
		var relocatedSlaves [](*inst.Instance)
		relocatedSlaves, successorInstance, err, errs = inst.RelocateSlaves(failedInstanceKey, &analysisEntry.AnalyzedInstanceMasterKey, "")
		topologyRecovery.AddErrors(errs)
		topologyRecovery.ParticipatingInstanceKeys.AddKey(analysisEntry.AnalyzedInstanceMasterKey)

		if len(relocatedSlaves) > 0 {
			recoveryResolved = true
			inst.AuditOperation("recover-dead-intermediate-master", failedInstanceKey, fmt.Sprintf("Relocated slaves under: %+v %d errors: %+v", successorInstance.Key, len(errs), errs))
		} else {
			err = log.Errorf("topology_recovery: RecoverDeadIntermediateMaster failed to match up any slave from %+v", *failedInstanceKey)
			topologyRecovery.AddError(err)
		}
	}
	if !recoveryResolved {
		successorInstance = nil
	}
	ResolveRecovery(topologyRecovery, successorInstance)
	return successorInstance, err
}

func checkAndRecoverDeadIntermediateMaster(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *TopologyRecovery, error) {
	if !(forceInstanceRecovery || analysisEntry.ClusterDetails.HasAutomatedIntermediateMasterRecovery) {
		return false, nil, nil
	}
	topologyRecovery, err := AttemptRecoveryRegistration(&analysisEntry, !forceInstanceRecovery, !forceInstanceRecovery)
	if topologyRecovery == nil {
		log.Debugf("topology_recovery: found an active or recent recovery on %+v. Will not issue another RecoverDeadIntermediateMaster.", analysisEntry.AnalyzedInstanceKey)
		return false, nil, err
	}

	recoverDeadIntermediateMasterCounter.Inc(1)
	promotedSlave, err := RecoverDeadIntermediateMaster(topologyRecovery, skipProcesses)
	if promotedSlave != nil {
		recoverDeadIntermediateMasterSuccessCounter.Inc(1)

		if !skipProcesses {
			topologyRecovery.SuccessorKey = &promotedSlave.Key
			executeProcesses(config.Config.PostIntermediateMasterFailoverProcesses, "PostIntermediateMasterFailoverProcesses", topologyRecovery, false)
		}
	} else {
		recoverDeadIntermediateMasterFailureCounter.Inc(1)
	}
	return true, topologyRecovery, err
}

func RecoverDeadCoMaster(topologyRecovery *TopologyRecovery, skipProcesses bool) (promotedSlave *inst.Instance, lostSlaves [](*inst.Instance), err error) {
	analysisEntry := &topologyRecovery.AnalysisEntry
	failedInstanceKey := &analysisEntry.AnalyzedInstanceKey
	otherCoMasterKey := &analysisEntry.AnalyzedInstanceMasterKey
	otherCoMaster, found, _ := inst.ReadInstance(otherCoMasterKey)
	if otherCoMaster == nil || !found {
		return nil, lostSlaves, topologyRecovery.AddError(log.Errorf("RecoverDeadCoMaster: could not read info for co-master %+v of %+v", *otherCoMasterKey, *failedInstanceKey))
	}
	inst.AuditOperation("recover-dead-co-master", failedInstanceKey, "problem found; will recover")
	if !skipProcesses {
		if err := executeProcesses(config.Config.PreFailoverProcesses, "PreFailoverProcesses", topologyRecovery, true); err != nil {
			return nil, lostSlaves, topologyRecovery.AddError(err)
		}
	}

	log.Debugf("topology_recovery: RecoverDeadCoMaster: will recover %+v", *failedInstanceKey)

	var coMasterRecoveryType MasterRecoveryType = MasterRecoveryPseudoGTID
	if analysisEntry.OracleGTIDImmediateTopology || analysisEntry.MariaDBGTIDImmediateTopology {
		coMasterRecoveryType = MasterRecoveryGTID
	}

	log.Debugf("topology_recovery: RecoverDeadCoMaster: coMasterRecoveryType=%+v", coMasterRecoveryType)

	switch coMasterRecoveryType {
	case MasterRecoveryGTID:
		{
			lostSlaves, _, promotedSlave, err = inst.RegroupSlavesGTID(failedInstanceKey, true, nil)
		}
	case MasterRecoveryPseudoGTID:
		{
			lostSlaves, _, _, promotedSlave, err = inst.RegroupSlavesPseudoGTIDIncludingSubSlavesOfBinlogServers(failedInstanceKey, true, nil, &topologyRecovery.PostponedFunctionsContainer)
		}
	}
	topologyRecovery.AddError(err)

	mustPromoteOtherCoMaster := config.Config.CoMasterRecoveryMustPromoteOtherCoMaster
	if !otherCoMaster.ReadOnly {
		log.Debugf("topology_recovery: RecoverDeadCoMaster: other co-master %+v is writeable hence has to be promoted", otherCoMaster.Key)
		mustPromoteOtherCoMaster = true
	}
	log.Debugf("topology_recovery: RecoverDeadCoMaster: mustPromoteOtherCoMaster? %+v", mustPromoteOtherCoMaster)

	if promotedSlave != nil {
		topologyRecovery.ParticipatingInstanceKeys.AddKey(promotedSlave.Key)
		if mustPromoteOtherCoMaster {
			log.Debugf("topology_recovery: mustPromoteOtherCoMaster. Verifying that %+v is/can be promoted", *otherCoMasterKey)
			promotedSlave, err = replacePromotedSlaveWithCandidate(failedInstanceKey, promotedSlave, otherCoMasterKey)
		} else {
			promotedSlave, err = replacePromotedSlaveWithCandidate(failedInstanceKey, promotedSlave, nil)

			if promotedSlave.DataCenter == otherCoMaster.DataCenter &&
				promotedSlave.PhysicalEnvironment == otherCoMaster.PhysicalEnvironment && false {
				promotedSlave, err = replacePromotedSlaveWithCandidate(failedInstanceKey, promotedSlave, otherCoMasterKey)
			}
		}
		topologyRecovery.AddError(err)
	}
	if promotedSlave != nil {
		if mustPromoteOtherCoMaster && !promotedSlave.Key.Equals(otherCoMasterKey) {
			topologyRecovery.AddError(log.Errorf("RecoverDeadCoMaster: could not manage to promote other-co-master %+v; was only able to promote %+v; CoMasterRecoveryMustPromoteOtherCoMaster is true, therefore failing", *otherCoMasterKey, promotedSlave.Key))
			promotedSlave = nil
		}
	}
	if promotedSlave != nil {
		topologyRecovery.ParticipatingInstanceKeys.AddKey(promotedSlave.Key)
	}

	if promotedSlave != nil && !promotedSlave.Key.Equals(otherCoMasterKey) {
		_, err = inst.DetachSlaveMasterHost(&promotedSlave.Key)
		topologyRecovery.AddError(log.Errore(err))
	}

	if promotedSlave != nil && len(lostSlaves) > 0 && config.Config.DetachLostSlavesAfterMasterFailover {
		postponedFunction := func() error {
			log.Debugf("topology_recovery: - RecoverDeadCoMaster: lost %+v slaves during recovery process; detaching them", len(lostSlaves))
			for _, slave := range lostSlaves {
				slave := slave
				inst.DetachSlaveOperation(&slave.Key)
			}
			return nil
		}
		topologyRecovery.AddPostponedFunction(postponedFunction)
	}
	if config.Config.MasterFailoverLostInstancesDowntimeMinutes > 0 {
		postponedFunction := func() error {
			inst.BeginDowntime(failedInstanceKey, inst.GetMaintenanceOwner(), "RecoverDeadCoMaster indicates this instance is lost", config.Config.MasterFailoverLostInstancesDowntimeMinutes*60)
			for _, slave := range lostSlaves {
				slave := slave
				inst.BeginDowntime(&slave.Key, inst.GetMaintenanceOwner(), "RecoverDeadCoMaster indicates this instance is lost", config.Config.MasterFailoverLostInstancesDowntimeMinutes*60)
			}
			return nil
		}
		topologyRecovery.AddPostponedFunction(postponedFunction)
	}

	return promotedSlave, lostSlaves, err
}

func checkAndRecoverDeadCoMaster(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *TopologyRecovery, error) {
	failedInstanceKey := &analysisEntry.AnalyzedInstanceKey
	if !(forceInstanceRecovery || analysisEntry.ClusterDetails.HasAutomatedMasterRecovery) {
		return false, nil, nil
	}
	topologyRecovery, err := AttemptRecoveryRegistration(&analysisEntry, !forceInstanceRecovery, !forceInstanceRecovery)
	if topologyRecovery == nil {
		log.Debugf("topology_recovery: found an active or recent recovery on %+v. Will not issue another RecoverDeadCoMaster.", analysisEntry.AnalyzedInstanceKey)
		return false, nil, err
	}

	recoverDeadCoMasterCounter.Inc(1)
	promotedSlave, lostSlaves, err := RecoverDeadCoMaster(topologyRecovery, skipProcesses)
	ResolveRecovery(topologyRecovery, promotedSlave)
	if promotedSlave == nil {
		inst.AuditOperation("recover-dead-co-master", failedInstanceKey, "Failure: no slave promoted.")
	} else {
		inst.AuditOperation("recover-dead-co-master", failedInstanceKey, fmt.Sprintf("promoted: %+v", promotedSlave.Key))
	}
	topologyRecovery.LostSlaves.AddInstances(lostSlaves)
	if promotedSlave != nil {
		recoverDeadCoMasterSuccessCounter.Inc(1)

		if config.Config.ApplyMySQLPromotionAfterMasterFailover {
			log.Debugf("topology_recovery: - RecoverDeadMaster: will apply MySQL changes to promoted master")
			inst.SetReadOnly(&promotedSlave.Key, false)
		}
		if !skipProcesses {
			topologyRecovery.SuccessorKey = &promotedSlave.Key
			executeProcesses(config.Config.PostMasterFailoverProcesses, "PostMasterFailoverProcesses", topologyRecovery, false)
		}
	} else {
		recoverDeadCoMasterFailureCounter.Inc(1)
	}
	return true, topologyRecovery, err
}

func checkAndRecoverGenericProblem(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *TopologyRecovery, error) {
	return false, nil, nil
}

func emergentlyReadTopologyInstance(instanceKey *inst.InstanceKey, analysisCode inst.AnalysisCode) {
	if existsInCacheError := emergencyReadTopologyInstanceMap.Add(instanceKey.DisplayString(), true, cache.DefaultExpiration); existsInCacheError != nil {
		return
	}
	go inst.ExecuteOnTopology(func() {
		inst.ReadTopologyInstance(instanceKey)
		inst.AuditOperation("emergently-read-topology-instance", instanceKey, string(analysisCode))
	})
}

func emergentlyReadTopologyInstanceSlaves(instanceKey *inst.InstanceKey, analysisCode inst.AnalysisCode) {
	slaves, err := inst.ReadSlaveInstancesIncludingBinlogServerSubSlaves(instanceKey)
	if err != nil {
		return
	}
	for _, slave := range slaves {
		go emergentlyReadTopologyInstance(&slave.Key, analysisCode)
	}
}

func checkAndExecuteFailureDetectionProcesses(analysisEntry inst.ReplicationAnalysis, skipProcesses bool) (processesExecutionAttempted bool, err error) {
	if ok, _ := AttemptFailureDetectionRegistration(&analysisEntry); !ok {
		return false, nil
	}
	log.Debugf("topology_recovery: detected %+v failure on %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey)
	if skipProcesses {
		return false, nil
	}
	err = executeProcesses(config.Config.OnFailureDetectionProcesses, "OnFailureDetectionProcesses", NewTopologyRecovery(analysisEntry), true)
	return true, err
}

func executeCheckAndRecoverFunction(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	var checkAndRecoverFunction func(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) = nil

	switch analysisEntry.Analysis {
	case inst.DeadMaster:
		checkAndRecoverFunction = checkAndRecoverDeadMaster
	case inst.DeadMasterAndSomeSlaves:
		checkAndRecoverFunction = checkAndRecoverDeadMaster
	case inst.DeadIntermediateMaster:
		checkAndRecoverFunction = checkAndRecoverDeadIntermediateMaster
	case inst.DeadIntermediateMasterAndSomeSlaves:
		checkAndRecoverFunction = checkAndRecoverDeadIntermediateMaster
	case inst.DeadIntermediateMasterWithSingleSlaveFailingToConnect:
		checkAndRecoverFunction = checkAndRecoverDeadIntermediateMaster
	case inst.AllIntermediateMasterSlavesFailingToConnectOrDead:
		checkAndRecoverFunction = checkAndRecoverDeadIntermediateMaster
	case inst.DeadCoMaster:
		checkAndRecoverFunction = checkAndRecoverDeadCoMaster
	case inst.DeadCoMasterAndSomeSlaves:
		checkAndRecoverFunction = checkAndRecoverDeadCoMaster
	case inst.DeadMasterAndSlaves:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceMasterKey, analysisEntry.Analysis)
	case inst.UnreachableMaster:
		go emergentlyReadTopologyInstanceSlaves(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
	case inst.AllMasterSlavesNotReplicating:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
	case inst.FirstTierSlaveFailingToConnectToMaster:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceMasterKey, analysisEntry.Analysis)
	case inst.UnreachableMasterWithStaleSlaves:
		checkAndRecoverFunction = checkAndRecoverGenericProblem
	case inst.AllMasterSlavesStale:
		checkAndRecoverFunction = checkAndRecoverGenericProblem
	}

	if checkAndRecoverFunction == nil {
		return false, nil, nil
	}
	log.Debugf("executeCheckAndRecoverFunction: proceeeding with %+v; skipProcesses: %+v", analysisEntry.AnalyzedInstanceKey, skipProcesses)

	if _, err := checkAndExecuteFailureDetectionProcesses(analysisEntry, skipProcesses); err != nil {
		return false, nil, err
	}

	recoveryAttempted, topologyRecovery, err = checkAndRecoverFunction(analysisEntry, candidateInstanceKey, forceInstanceRecovery, skipProcesses)
	if !recoveryAttempted {
		return recoveryAttempted, topologyRecovery, err
	}
	if topologyRecovery == nil {
		return recoveryAttempted, topologyRecovery, err
	}
	if !skipProcesses {
		if topologyRecovery.SuccessorKey == nil {
			executeProcesses(config.Config.PostUnsuccessfulFailoverProcesses, "PostUnsuccessfulFailoverProcesses", topologyRecovery, false)
		} else {
			executeProcesses(config.Config.PostFailoverProcesses, "PostFailoverProcesses", topologyRecovery, false)
		}
	}
	topologyRecovery.InvokePostponed()
	return recoveryAttempted, topologyRecovery, err
}

func CheckAndRecover(specificInstance *inst.InstanceKey, candidateInstanceKey *inst.InstanceKey, skipProcesses bool) (recoveryAttempted bool, promotedSlaveKey *inst.InstanceKey, err error) {
	replicationAnalysis, err := inst.GetReplicationAnalysis("", true, true)
	if err != nil {
		return false, nil, log.Errore(err)
	}
	if *config.RuntimeCLIFlags.Noop {
		log.Debugf("--noop provided; will not execute processes")
		skipProcesses = true
	}
	for _, analysisEntry := range replicationAnalysis {
		if specificInstance != nil {
			if !specificInstance.Equals(&analysisEntry.AnalyzedInstanceKey) {
				continue
			}
		}
		if analysisEntry.IsDowntimed && specificInstance == nil {
			continue
		}

		if specificInstance != nil {
			var topologyRecovery *TopologyRecovery
			recoveryAttempted, topologyRecovery, err = executeCheckAndRecoverFunction(analysisEntry, candidateInstanceKey, true, skipProcesses)
			if topologyRecovery != nil {
				promotedSlaveKey = topologyRecovery.SuccessorKey
			}
		} else {
			go executeCheckAndRecoverFunction(analysisEntry, candidateInstanceKey, false, skipProcesses)
		}
	}
	return recoveryAttempted, promotedSlaveKey, err
}

func ForceExecuteRecovery(clusterName string, analysisCode inst.AnalysisCode, failedInstanceKey *inst.InstanceKey, candidateInstanceKey *inst.InstanceKey, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	clusterInfo, err := inst.ReadClusterInfo(clusterName)
	if err != nil {
		return recoveryAttempted, topologyRecovery, err
	}

	analysisEntry := inst.ReplicationAnalysis{
		Analysis:            analysisCode,
		ClusterDetails:      *clusterInfo,
		AnalyzedInstanceKey: *failedInstanceKey,
	}
	return executeCheckAndRecoverFunction(analysisEntry, candidateInstanceKey, true, skipProcesses)
}
