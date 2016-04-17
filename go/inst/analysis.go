/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package inst

import (
	"strings"
)

type AnalysisCode string
type StructureAnalysisCode string

const (
	NoProblem                                             AnalysisCode = "NoProblem"
	DeadMasterWithoutSlaves                                            = "DeadMasterWithoutSlaves"
	DeadMaster                                                         = "DeadMaster"
	DeadMasterAndSlaves                                                = "DeadMasterAndSlaves"
	DeadMasterAndSomeSlaves                                            = "DeadMasterAndSomeSlaves"
	UnreachableMasterWithStaleSlaves                                   = "UnreachableMasterWithStaleSlaves"
	UnreachableMaster                                                  = "UnreachableMaster"
	MasterSingleSlaveNotReplicating                                    = "MasterSingleSlaveNotReplicating"
	MasterSingleSlaveDead                                              = "MasterSingleSlaveDead"
	AllMasterSlavesNotReplicating                                      = "AllMasterSlavesNotReplicating"
	AllMasterSlavesNotReplicatingOrDead                                = "AllMasterSlavesNotReplicatingOrDead"
	AllMasterSlavesStale                                               = "AllMasterSlavesStale"
	MasterWithoutSlaves                                                = "MasterWithoutSlaves"
	DeadCoMaster                                                       = "DeadCoMaster"
	DeadCoMasterAndSomeSlaves                                          = "DeadCoMasterAndSomeSlaves"
	UnreachableCoMaster                                                = "UnreachableCoMaster"
	AllCoMasterSlavesNotReplicating                                    = "AllCoMasterSlavesNotReplicating"
	DeadIntermediateMaster                                             = "DeadIntermediateMaster"
	DeadIntermediateMasterWithSingleSlave                              = "DeadIntermediateMasterWithSingleSlave"
	DeadIntermediateMasterWithSingleSlaveFailingToConnect              = "DeadIntermediateMasterWithSingleSlaveFailingToConnect"
	DeadIntermediateMasterAndSomeSlaves                                = "DeadIntermediateMasterAndSomeSlaves"
	UnreachableIntermediateMaster                                      = "UnreachableIntermediateMaster"
	AllIntermediateMasterSlavesFailingToConnectOrDead                  = "AllIntermediateMasterSlavesFailingToConnectOrDead"
	AllIntermediateMasterSlavesNotReplicating                          = "AllIntermediateMasterSlavesNotReplicating"
	FirstTierSlaveFailingToConnectToMaster                             = "FirstTierSlaveFailingToConnectToMaster"
	BinlogServerFailingToConnectToMaster                               = "BinlogServerFailingToConnectToMaster"
)

const (
	StatementAndMixedLoggingSlavesStructureWarning StructureAnalysisCode = "StatementAndMixedLoggingSlavesStructureWarning"
	StatementAndRowLoggingSlavesStructureWarning                         = "StatementAndRowLoggingSlavesStructureWarning"
	MixedAndRowLoggingSlavesStructureWarning                             = "MixedAndRowLoggingSlavesStructureWarning"
	MultipleMajorVersionsLoggingSlaves                                   = "MultipleMajorVersionsLoggingSlaves"
)

type ReplicationAnalysis struct {
	AnalyzedInstanceKey                     InstanceKey
	AnalyzedInstanceMasterKey               InstanceKey
	ClusterDetails                          ClusterInfo
	IsMaster                                bool
	IsCoMaster                              bool
	LastCheckValid                          bool
	CountSlaves                             uint
	CountValidSlaves                        uint
	CountValidReplicatingSlaves             uint
	CountSlavesFailingToConnectToMaster     uint
	CountStaleSlaves                        uint
	ReplicationDepth                        uint
	SlaveHosts                              InstanceKeyMap
	IsFailingToConnectToMaster              bool
	Analysis                                AnalysisCode
	Description                             string
	StructureAnalysis                       []StructureAnalysisCode
	IsDowntimed                             bool
	DowntimeEndTimestamp                    string
	DowntimeRemainingSeconds                int
	IsBinlogServer                          bool
	PseudoGTIDImmediateTopology             bool
	OracleGTIDImmediateTopology             bool
	MariaDBGTIDImmediateTopology            bool
	BinlogServerImmediateTopology           bool
	CountStatementBasedLoggingSlaves        uint
	CountMixedBasedLoggingSlaves            uint
	CountRowBasedLoggingSlaves              uint
	CountDistinctMajorVersionsLoggingSlaves uint
}

type ReplicationAnalysisChangelog struct {
	AnalyzedInstanceKey InstanceKey
	Changelog           string
}

func (this *ReplicationAnalysis) ReadSlaveHostsFromString(slaveHostsString string) error {
	this.SlaveHosts = *NewInstanceKeyMap()
	return this.SlaveHosts.ReadCommaDelimitedList(slaveHostsString)
}

func (this *ReplicationAnalysis) AnalysisString() string {
	result := []string{}
	if this.Analysis != NoProblem {
		result = append(result, string(this.Analysis))
	}
	for _, structureAnalysis := range this.StructureAnalysis {
		result = append(result, string(structureAnalysis))
	}
	return strings.Join(result, ", ")
}
