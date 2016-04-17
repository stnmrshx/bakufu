/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package inst

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/stnmrshx/golib/log"
	"github.com/stnmrshx/golib/math"
	"github.com/stnmrshx/bakufu/go/config"
)

func getASCIITopologyEntry(depth int, instance *Instance, replicationMap map[*Instance]([]*Instance), extendedOutput bool) []string {
	if instance == nil {
		return []string{}
	}
	if instance.IsCoMaster && depth > 1 {
		return []string{}
	}
	prefix := ""
	if depth > 0 {
		prefix = strings.Repeat(" ", (depth-1)*2)
		if instance.SlaveRunning() && instance.IsLastCheckValid && instance.IsRecentlyChecked {
			prefix += "+ "
		} else {
			prefix += "- "
		}
	}
	entry := fmt.Sprintf("%s%s", prefix, instance.Key.DisplayString())
	if extendedOutput {
		entry = fmt.Sprintf("%s %s", entry, instance.HumanReadableDescription())
	}
	result := []string{entry}
	for _, slave := range replicationMap[instance] {
		slavesResult := getASCIITopologyEntry(depth+1, slave, replicationMap, extendedOutput)
		result = append(result, slavesResult...)
	}
	return result
}

func ASCIITopology(clusterName string, historyTimestampPattern string) (result string, err error) {
	var instances [](*Instance)
	if historyTimestampPattern == "" {
		instances, err = ReadClusterInstances(clusterName)
	} else {
		instances, err = ReadHistoryClusterInstances(clusterName, historyTimestampPattern)
	}
	if err != nil {
		return "", err
	}

	instancesMap := make(map[InstanceKey](*Instance))
	for _, instance := range instances {
		log.Debugf("instanceKey: %+v", instance.Key)
		instancesMap[instance.Key] = instance
	}

	replicationMap := make(map[*Instance]([]*Instance))
	var masterInstance *Instance
	for _, instance := range instances {
		master, ok := instancesMap[instance.MasterKey]
		if ok {
			if _, ok := replicationMap[master]; !ok {
				replicationMap[master] = [](*Instance){}
			}
			replicationMap[master] = append(replicationMap[master], instance)
		} else {
			masterInstance = instance
		}
	}
	var entries []string
	if masterInstance != nil {
		entries = getASCIITopologyEntry(0, masterInstance, replicationMap, historyTimestampPattern == "")
	} else {
		for _, instance := range instances {
			if instance.IsCoMaster {
				entries = append(entries, getASCIITopologyEntry(1, instance, replicationMap, historyTimestampPattern == "")...)
			}
		}
	}
	{
		maxIndent := 0
		for _, entry := range entries {
			maxIndent = math.MaxInt(maxIndent, strings.Index(entry, "["))
		}
		for i, entry := range entries {
			entryIndent := strings.Index(entry, "[")
			if maxIndent > entryIndent {
				tokens := strings.Split(entry, "[")
				newEntry := fmt.Sprintf("%s%s[%s", tokens[0], strings.Repeat(" ", maxIndent-entryIndent), tokens[1])
				entries[i] = newEntry
			}
		}
	}
	result = strings.Join(entries, "\n")
	return result, nil
}

func GetInstanceMaster(instance *Instance) (*Instance, error) {
	master, err := ReadTopologyInstance(&instance.MasterKey)
	return master, err
}

func InstancesAreSiblings(instance0, instance1 *Instance) bool {
	if !instance0.IsSlave() {
		return false
	}
	if !instance1.IsSlave() {
		return false
	}
	if instance0.Key.Equals(&instance1.Key) {
		return false
	}
	return instance0.MasterKey.Equals(&instance1.MasterKey)
}

func InstanceIsMasterOf(allegedMaster, allegedSlave *Instance) bool {
	if !allegedSlave.IsSlave() {
		return false
	}
	if allegedMaster.Key.Equals(&allegedSlave.Key) {
		return false
	}
	return allegedMaster.Key.Equals(&allegedSlave.MasterKey)
}

func MoveEquivalent(instanceKey, otherKey *InstanceKey) (*Instance, error) {
	instance, found, err := ReadInstance(instanceKey)
	if err != nil || !found {
		return instance, err
	}
	if instance.Key.Equals(otherKey) {
		return instance, fmt.Errorf("MoveEquivalent: attempt to move an instance below itself %+v", instance.Key)
	}

	instanceCoordinates := &InstanceBinlogCoordinates{Key: instance.MasterKey, Coordinates: instance.ExecBinlogCoordinates}
	binlogCoordinates, err := GetEquivalentBinlogCoordinatesFor(instanceCoordinates, otherKey)
	if err != nil {
		return instance, err
	}
	if binlogCoordinates == nil {
		return instance, fmt.Errorf("No equivalent coordinates found for %+v replicating from %+v at %+v", instance.Key, instance.MasterKey, instance.ExecBinlogCoordinates)
	}
	knownExecBinlogCoordinates := instance.ExecBinlogCoordinates
	instance, err = StopSlave(instanceKey)
	if err != nil {
		goto Cleanup
	}
	if !instance.ExecBinlogCoordinates.Equals(&knownExecBinlogCoordinates) {
		err = fmt.Errorf("MoveEquivalent(): ExecBinlogCoordinates changed after stopping replication on %+v; aborting", instance.Key)
		goto Cleanup
	}
	instance, err = ChangeMasterTo(instanceKey, otherKey, binlogCoordinates, false, GTIDHintNeutral)

Cleanup:
	instance, _ = StartSlave(instanceKey)

	if err == nil {
		message := fmt.Sprintf("moved %+v via equivalence coordinates below %+v", *instanceKey, *otherKey)
		log.Debugf(message)
		AuditOperation("move-equivalent", instanceKey, message)
	}
	return instance, err
}

func MoveUp(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	if !instance.IsSlave() {
		return instance, fmt.Errorf("instance is not a slave: %+v", instanceKey)
	}
	rinstance, _, _ := ReadInstance(&instance.Key)
	if canMove, merr := rinstance.CanMove(); !canMove {
		return instance, merr
	}
	master, err := GetInstanceMaster(instance)
	if err != nil {
		return instance, log.Errorf("Cannot GetInstanceMaster() for %+v. error=%+v", instance.Key, err)
	}

	if !master.IsSlave() {
		return instance, fmt.Errorf("master is not a slave itself: %+v", master.Key)
	}

	if canReplicate, err := instance.CanReplicateFrom(master); canReplicate == false {
		return instance, err
	}
	if master.IsBinlogServer() {
		return Repoint(instanceKey, &master.MasterKey, GTIDHintDeny)
	}

	log.Infof("Will move %+v up the topology", *instanceKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "move up"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", *instanceKey)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}
	if maintenanceToken, merr := BeginMaintenance(&master.Key, GetMaintenanceOwner(), fmt.Sprintf("child %+v moves up", *instanceKey)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", master.Key)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	if !instance.UsingMariaDBGTID {
		master, err = StopSlave(&master.Key)
		if err != nil {
			goto Cleanup
		}
	}

	instance, err = StopSlave(instanceKey)
	if err != nil {
		goto Cleanup
	}

	if !instance.UsingMariaDBGTID {
		instance, err = StartSlaveUntilMasterCoordinates(instanceKey, &master.SelfBinlogCoordinates)
		if err != nil {
			goto Cleanup
		}
	}

	instance, err = ChangeMasterTo(instanceKey, &master.MasterKey, &master.ExecBinlogCoordinates, true, GTIDHintDeny)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartSlave(instanceKey)
	if !instance.UsingMariaDBGTID {
		master, _ = StartSlave(&master.Key)
	}
	if err != nil {
		return instance, log.Errore(err)
	}
	AuditOperation("move-up", instanceKey, fmt.Sprintf("moved up %+v. Previous master: %+v", *instanceKey, master.Key))

	return instance, err
}

func MoveUpSlaves(instanceKey *InstanceKey, pattern string) ([](*Instance), *Instance, error, []error) {
	res := [](*Instance){}
	errs := []error{}
	slaveMutex := make(chan bool, 1)
	var barrier chan *InstanceKey

	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return res, nil, err, errs
	}
	if !instance.IsSlave() {
		return res, instance, fmt.Errorf("instance is not a slave: %+v", instanceKey), errs
	}
	_, err = GetInstanceMaster(instance)
	if err != nil {
		return res, instance, log.Errorf("Cannot GetInstanceMaster() for %+v. error=%+v", instance.Key, err), errs
	}

	if instance.IsBinlogServer() {
		slaves, err, errors := RepointSlavesTo(instanceKey, pattern, &instance.MasterKey)
		return slaves, instance, err, errors
	}

	slaves, err := ReadSlaveInstances(instanceKey)
	if err != nil {
		return res, instance, err, errs
	}
	slaves = filterInstancesByPattern(slaves, pattern)
	if len(slaves) == 0 {
		return res, instance, nil, errs
	}
	log.Infof("Will move slaves of %+v up the topology", *instanceKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "move up slaves"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", *instanceKey)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}
	for _, slave := range slaves {
		if maintenanceToken, merr := BeginMaintenance(&slave.Key, GetMaintenanceOwner(), fmt.Sprintf("%+v moves up", slave.Key)); merr != nil {
			err = fmt.Errorf("Cannot begin maintenance on %+v", slave.Key)
			goto Cleanup
		} else {
			defer EndMaintenance(maintenanceToken)
		}
	}

	instance, err = StopSlave(instanceKey)
	if err != nil {
		goto Cleanup
	}

	barrier = make(chan *InstanceKey)
	for _, slave := range slaves {
		slave := slave
		go func() {
			defer func() {
				defer func() { barrier <- &slave.Key }()
				StartSlave(&slave.Key)
			}()

			var slaveErr error
			ExecuteOnTopology(func() {
				if canReplicate, err := slave.CanReplicateFrom(instance); canReplicate == false || err != nil {
					slaveErr = err
					return
				}
				if instance.IsBinlogServer() {
					slave, err = Repoint(&slave.Key, instanceKey, GTIDHintDeny)
					if err != nil {
						slaveErr = err
						return
					}
				} else {
					slave, err = StopSlave(&slave.Key)
					if err != nil {
						slaveErr = err
						return
					}
					slave, err = StartSlaveUntilMasterCoordinates(&slave.Key, &instance.SelfBinlogCoordinates)
					if err != nil {
						slaveErr = err
						return
					}

					slave, err = ChangeMasterTo(&slave.Key, &instance.MasterKey, &instance.ExecBinlogCoordinates, false, GTIDHintDeny)
					if err != nil {
						slaveErr = err
						return
					}
				}
			})

			func() {
				slaveMutex <- true
				defer func() { <-slaveMutex }()
				if slaveErr == nil {
					res = append(res, slave)
				} else {
					errs = append(errs, slaveErr)
				}
			}()
		}()
	}
	for range slaves {
		<-barrier
	}

Cleanup:
	instance, _ = StartSlave(instanceKey)
	if err != nil {
		return res, instance, log.Errore(err), errs
	}
	if len(errs) == len(slaves) {
		return res, instance, log.Error("Error on all operations"), errs
	}
	AuditOperation("move-up-slaves", instanceKey, fmt.Sprintf("moved up %d/%d slaves of %+v. New master: %+v", len(res), len(slaves), *instanceKey, instance.MasterKey))

	return res, instance, err, errs
}

func MoveBelow(instanceKey, siblingKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	sibling, err := ReadTopologyInstance(siblingKey)
	if err != nil {
		return instance, err
	}

	if sibling.IsBinlogServer() {
		return Repoint(instanceKey, &sibling.Key, GTIDHintDeny)
	}

	rinstance, _, _ := ReadInstance(&instance.Key)
	if canMove, merr := rinstance.CanMove(); !canMove {
		return instance, merr
	}

	rinstance, _, _ = ReadInstance(&sibling.Key)
	if canMove, merr := rinstance.CanMove(); !canMove {
		return instance, merr
	}
	if !InstancesAreSiblings(instance, sibling) {
		return instance, fmt.Errorf("instances are not siblings: %+v, %+v", *instanceKey, *siblingKey)
	}

	if canReplicate, err := instance.CanReplicateFrom(sibling); !canReplicate {
		return instance, err
	}
	log.Infof("Will move %+v below %+v", instanceKey, siblingKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), fmt.Sprintf("move below %+v", *siblingKey)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", *instanceKey)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}
	if maintenanceToken, merr := BeginMaintenance(siblingKey, GetMaintenanceOwner(), fmt.Sprintf("%+v moves below this", *instanceKey)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", *siblingKey)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	instance, err = StopSlave(instanceKey)
	if err != nil {
		goto Cleanup
	}

	sibling, err = StopSlave(siblingKey)
	if err != nil {
		goto Cleanup
	}
	if instance.ExecBinlogCoordinates.SmallerThan(&sibling.ExecBinlogCoordinates) {
		instance, err = StartSlaveUntilMasterCoordinates(instanceKey, &sibling.ExecBinlogCoordinates)
		if err != nil {
			goto Cleanup
		}
	} else if sibling.ExecBinlogCoordinates.SmallerThan(&instance.ExecBinlogCoordinates) {
		sibling, err = StartSlaveUntilMasterCoordinates(siblingKey, &instance.ExecBinlogCoordinates)
		if err != nil {
			goto Cleanup
		}
	}

	instance, err = ChangeMasterTo(instanceKey, &sibling.Key, &sibling.SelfBinlogCoordinates, false, GTIDHintDeny)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartSlave(instanceKey)
	sibling, _ = StartSlave(siblingKey)

	if err != nil {
		return instance, log.Errore(err)
	}
	AuditOperation("move-below", instanceKey, fmt.Sprintf("moved %+v below %+v", *instanceKey, *siblingKey))

	return instance, err
}

func canMoveViaGTID(instance, otherInstance *Instance) (isOracleGTID bool, isMariaDBGTID, canMove bool) {
	isOracleGTID = (instance.UsingOracleGTID && otherInstance.SupportsOracleGTID)
	isMariaDBGTID = (instance.UsingMariaDBGTID && otherInstance.IsMariaDB())

	return isOracleGTID, isMariaDBGTID, isOracleGTID || isMariaDBGTID
}

func moveInstanceBelowViaGTID(instance, otherInstance *Instance) (*Instance, error) {
	_, _, canMove := canMoveViaGTID(instance, otherInstance)

	instanceKey := &instance.Key
	otherInstanceKey := &otherInstance.Key
	if !canMove {
		return instance, fmt.Errorf("Cannot move via GTID as not both instances use GTID: %+v, %+v", *instanceKey, *otherInstanceKey)
	}

	var err error

	rinstance, _, _ := ReadInstance(&instance.Key)
	if canMove, merr := rinstance.CanMoveViaMatch(); !canMove {
		return instance, merr
	}

	if canReplicate, err := instance.CanReplicateFrom(otherInstance); !canReplicate {
		return instance, err
	}
	log.Infof("Will move %+v below %+v via GTID", instanceKey, otherInstanceKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), fmt.Sprintf("move below %+v", *otherInstanceKey)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", *instanceKey)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	instance, err = StopSlave(instanceKey)
	if err != nil {
		goto Cleanup
	}

	instance, err = ChangeMasterTo(instanceKey, &otherInstance.Key, &otherInstance.SelfBinlogCoordinates, false, GTIDHintForce)
	if err != nil {
		goto Cleanup
	}
Cleanup:
	instance, _ = StartSlave(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}
	AuditOperation("move-below-gtid", instanceKey, fmt.Sprintf("moved %+v below %+v", *instanceKey, *otherInstanceKey))

	return instance, err
}

func MoveBelowGTID(instanceKey, otherKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	other, err := ReadTopologyInstance(otherKey)
	if err != nil {
		return instance, err
	}
	return moveInstanceBelowViaGTID(instance, other)
}

func moveSlavesViaGTID(slaves [](*Instance), other *Instance) (movedSlaves [](*Instance), unmovedSlaves [](*Instance), err error, errs []error) {
	slaves = RemoveInstance(slaves, &other.Key)
	if len(slaves) == 0 {
		return movedSlaves, unmovedSlaves, nil, errs
	}

	log.Infof("Will move %+v slaves below %+v via GTID", len(slaves), other.Key)

	barrier := make(chan *InstanceKey)
	slaveMutex := make(chan bool, 1)
	for _, slave := range slaves {
		slave := slave

		go func() {
			defer func() { barrier <- &slave.Key }()
			ExecuteOnTopology(func() {
				var slaveErr error
				if _, _, canMove := canMoveViaGTID(slave, other); canMove {
					slave, slaveErr = moveInstanceBelowViaGTID(slave, other)
				} else {
					slaveErr = fmt.Errorf("%+v cannot move below %+v via GTID", slave.Key, other.Key)
				}
				func() {
					slaveMutex <- true
					defer func() { <-slaveMutex }()
					if slaveErr == nil {
						movedSlaves = append(movedSlaves, slave)
					} else {
						unmovedSlaves = append(unmovedSlaves, slave)
						errs = append(errs, slaveErr)
					}
				}()
			})
		}()
	}
	for range slaves {
		<-barrier
	}
	if len(errs) == len(slaves) {
		return movedSlaves, unmovedSlaves, fmt.Errorf("moveSlavesViaGTID: Error on all %+v operations", len(errs)), errs
	}
	AuditOperation("move-slaves-gtid", &other.Key, fmt.Sprintf("moved %d/%d slaves below %+v via GTID", len(movedSlaves), len(slaves), other.Key))

	return movedSlaves, unmovedSlaves, err, errs
}

func MoveSlavesGTID(masterKey *InstanceKey, belowKey *InstanceKey, pattern string) (movedSlaves [](*Instance), unmovedSlaves [](*Instance), err error, errs []error) {
	belowInstance, err := ReadTopologyInstance(belowKey)
	if err != nil {
		return movedSlaves, unmovedSlaves, err, errs
	}

	slaves, err := ReadSlaveInstancesIncludingBinlogServerSubSlaves(masterKey)
	if err != nil {
		return movedSlaves, unmovedSlaves, err, errs
	}
	slaves = filterInstancesByPattern(slaves, pattern)
	movedSlaves, unmovedSlaves, err, errs = moveSlavesViaGTID(slaves, belowInstance)
	if err != nil {
		log.Errore(err)
	}

	if len(unmovedSlaves) > 0 {
		err = fmt.Errorf("MoveSlavesGTID: only moved %d out of %d slaves of %+v; error is: %+v", len(movedSlaves), len(slaves), *masterKey, err)
	}

	return movedSlaves, unmovedSlaves, err, errs
}

func Repoint(instanceKey *InstanceKey, masterKey *InstanceKey, gtidHint OperationGTIDHint) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	if !instance.IsSlave() {
		return instance, fmt.Errorf("instance is not a slave: %+v", *instanceKey)
	}

	if masterKey == nil {
		masterKey = &instance.MasterKey
	}
	master, err := ReadTopologyInstance(masterKey)
	masterIsAccessible := (err == nil)
	if !masterIsAccessible {
		master, _, err = ReadInstance(masterKey)
		if err != nil {
			return instance, err
		}
	}
	if canReplicate, err := instance.CanReplicateFrom(master); !canReplicate {
		return instance, err
	}

	if master.IsBinlogServer() {
		if !instance.ExecBinlogCoordinates.SmallerThanOrEquals(&master.SelfBinlogCoordinates) {
			return instance, fmt.Errorf("repoint: binlog server %+v is not sufficiently up to date to repoint %+v below it", *masterKey, *instanceKey)
		}
	}

	log.Infof("Will repoint %+v to master %+v", *instanceKey, *masterKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "repoint"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", *instanceKey)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	instance, err = StopSlave(instanceKey)
	if err != nil {
		goto Cleanup
	}

	if instance.ExecBinlogCoordinates.IsEmpty() {
		instance.ExecBinlogCoordinates.LogFile = "bakufu-unknown-log-file"
	}
	instance, err = ChangeMasterTo(instanceKey, masterKey, &instance.ExecBinlogCoordinates, !masterIsAccessible, gtidHint)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartSlave(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}
	AuditOperation("repoint", instanceKey, fmt.Sprintf("slave %+v repointed to master: %+v", *instanceKey, *masterKey))

	return instance, err

}

func RepointTo(slaves [](*Instance), belowKey *InstanceKey) ([](*Instance), error, []error) {
	res := [](*Instance){}
	errs := []error{}

	slaves = RemoveInstance(slaves, belowKey)
	if len(slaves) == 0 {
		return res, nil, errs
	}
	if belowKey == nil {
		return res, log.Errorf("RepointTo received nil belowKey"), errs
	}

	log.Infof("Will repoint %+v slaves below %+v", len(slaves), *belowKey)
	barrier := make(chan *InstanceKey)
	slaveMutex := make(chan bool, 1)
	for _, slave := range slaves {
		slave := slave

		go func() {
			defer func() { barrier <- &slave.Key }()
			ExecuteOnTopology(func() {
				slave, slaveErr := Repoint(&slave.Key, belowKey, GTIDHintNeutral)

				func() {
					slaveMutex <- true
					defer func() { <-slaveMutex }()
					if slaveErr == nil {
						res = append(res, slave)
					} else {
						errs = append(errs, slaveErr)
					}
				}()
			})
		}()
	}
	for range slaves {
		<-barrier
	}

	if len(errs) == len(slaves) {
		return res, log.Error("Error on all operations"), errs
	}
	AuditOperation("repoint-to", belowKey, fmt.Sprintf("repointed %d/%d slaves to %+v", len(res), len(slaves), *belowKey))

	return res, nil, errs
}

func RepointSlavesTo(instanceKey *InstanceKey, pattern string, belowKey *InstanceKey) ([](*Instance), error, []error) {
	res := [](*Instance){}
	errs := []error{}

	slaves, err := ReadSlaveInstances(instanceKey)
	if err != nil {
		return res, err, errs
	}
	slaves = RemoveInstance(slaves, belowKey)
	slaves = filterInstancesByPattern(slaves, pattern)
	if len(slaves) == 0 {
		return res, nil, errs
	}
	if belowKey == nil {
		belowKey = &slaves[0].MasterKey
	}
	log.Infof("Will repoint slaves of %+v to %+v", *instanceKey, *belowKey)
	return RepointTo(slaves, belowKey)
}

func RepointSlaves(instanceKey *InstanceKey, pattern string) ([](*Instance), error, []error) {
	return RepointSlavesTo(instanceKey, pattern, nil)
}

func MakeCoMaster(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	if canMove, merr := instance.CanMove(); !canMove {
		return instance, merr
	}
	master, err := GetInstanceMaster(instance)
	if err != nil {
		return instance, err
	}
	log.Debugf("Will check whether %+v's master (%+v) can become its co-master", instance.Key, master.Key)
	if canMove, merr := master.CanMoveAsCoMaster(); !canMove {
		return instance, merr
	}
	if instanceKey.Equals(&master.MasterKey) {
		return instance, fmt.Errorf("instance %+v is already co master of %+v", instance.Key, master.Key)
	}
	if !instance.ReadOnly {
		return instance, fmt.Errorf("instance %+v is not read-only; first make it read-only before making it co-master", instance.Key)
	}
	if master.IsCoMaster {
		otherCoMaster, found, _ := ReadInstance(&master.MasterKey)
		if found && otherCoMaster.IsLastCheckValid && !otherCoMaster.ReadOnly {
			return instance, fmt.Errorf("master %+v is already co-master with %+v, and %+v is alive, and not read-only; cowardly refusing to demote it. Please set it as read-only beforehand", master.Key, otherCoMaster.Key, otherCoMaster.Key)
		}
	} else if _, found, _ := ReadInstance(&master.MasterKey); found {
		return instance, fmt.Errorf("%+v is not a real master; it replicates from: %+v", master.Key, master.MasterKey)
	}
	if canReplicate, err := master.CanReplicateFrom(instance); !canReplicate {
		return instance, err
	}
	log.Infof("Will make %+v co-master of %+v", instanceKey, master.Key)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), fmt.Sprintf("make co-master of %+v", master.Key)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", *instanceKey)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}
	if maintenanceToken, merr := BeginMaintenance(&master.Key, GetMaintenanceOwner(), fmt.Sprintf("%+v turns into co-master of this", *instanceKey)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", master.Key)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	if master.IsSlave() {
		master, err = StopSlave(&master.Key)
		if err != nil {
			goto Cleanup
		}
	}
	if instance.ReplicationCredentialsAvailable && !master.HasReplicationCredentials {
		replicationUser, replicationPassword, err := ReadReplicationCredentials(&instance.Key)
		if err != nil {
			goto Cleanup
		}
		log.Debugf("Got credentials from a replica. will now apply")
		_, err = ChangeMasterCredentials(&master.Key, replicationUser, replicationPassword)
		if err != nil {
			goto Cleanup
		}
	}
	master, err = ChangeMasterTo(&master.Key, instanceKey, &instance.SelfBinlogCoordinates, false, GTIDHintNeutral)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	master, _ = StartSlave(&master.Key)
	if err != nil {
		return instance, log.Errore(err)
	}
	AuditOperation("make-co-master", instanceKey, fmt.Sprintf("%+v made co-master of %+v", *instanceKey, master.Key))

	return instance, err
}

func ResetSlaveOperation(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}

	log.Infof("Will reset slave on %+v", instanceKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "reset slave"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", *instanceKey)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	if instance.IsSlave() {
		instance, err = StopSlave(instanceKey)
		if err != nil {
			goto Cleanup
		}
	}

	instance, err = ResetSlave(instanceKey)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartSlave(instanceKey)

	if err != nil {
		return instance, log.Errore(err)
	}

	AuditOperation("reset-slave", instanceKey, fmt.Sprintf("%+v replication reset", *instanceKey))

	return instance, err
}

func DetachSlaveOperation(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}

	log.Infof("Will detach %+v", instanceKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "detach slave"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", *instanceKey)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	if instance.IsSlave() {
		instance, err = StopSlave(instanceKey)
		if err != nil {
			goto Cleanup
		}
	}

	instance, err = DetachSlave(instanceKey)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartSlave(instanceKey)

	if err != nil {
		return instance, log.Errore(err)
	}

	AuditOperation("detach-slave", instanceKey, fmt.Sprintf("%+v replication detached", *instanceKey))

	return instance, err
}

func ReattachSlaveOperation(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}

	log.Infof("Will reattach %+v", instanceKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "detach slave"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", *instanceKey)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	if instance.IsSlave() {
		instance, err = StopSlave(instanceKey)
		if err != nil {
			goto Cleanup
		}
	}

	instance, err = ReattachSlave(instanceKey)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartSlave(instanceKey)

	if err != nil {
		return instance, log.Errore(err)
	}

	AuditOperation("reattach-slave", instanceKey, fmt.Sprintf("%+v replication reattached", *instanceKey))

	return instance, err
}

func DetachSlaveMasterHost(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	if !instance.IsSlave() {
		return instance, fmt.Errorf("instance is not a slave: %+v", *instanceKey)
	}
	if instance.MasterKey.IsDetached() {
		return instance, fmt.Errorf("instance already detached: %+v", *instanceKey)
	}
	detachedMasterKey := instance.MasterKey.DetachedKey()

	log.Infof("Will detach master host on %+v. Detached key is %+v", *instanceKey, *detachedMasterKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "detach-slave-master-host"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", *instanceKey)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	instance, err = StopSlave(instanceKey)
	if err != nil {
		goto Cleanup
	}

	instance, err = ChangeMasterTo(instanceKey, detachedMasterKey, &instance.ExecBinlogCoordinates, true, GTIDHintNeutral)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartSlave(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}
	AuditOperation("repoint", instanceKey, fmt.Sprintf("slave %+v detached from master into %+v", *instanceKey, *detachedMasterKey))

	return instance, err
}

func ReattachSlaveMasterHost(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	if !instance.IsSlave() {
		return instance, fmt.Errorf("instance is not a slave: %+v", *instanceKey)
	}
	if !instance.MasterKey.IsDetached() {
		return instance, fmt.Errorf("instance does not seem to be detached: %+v", *instanceKey)
	}

	reattachedMasterKey := instance.MasterKey.ReattachedKey()

	log.Infof("Will reattach master host on %+v. Reattached key is %+v", *instanceKey, *reattachedMasterKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "reattach-slave-master-host"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", *instanceKey)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	instance, err = StopSlave(instanceKey)
	if err != nil {
		goto Cleanup
	}

	instance, err = ChangeMasterTo(instanceKey, reattachedMasterKey, &instance.ExecBinlogCoordinates, true, GTIDHintNeutral)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartSlave(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}
	AuditOperation("repoint", instanceKey, fmt.Sprintf("slave %+v reattached to master %+v", *instanceKey, *reattachedMasterKey))

	return instance, err
}

func EnableGTID(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	if instance.UsingGTID() {
		return instance, fmt.Errorf("%+v already uses GTID", *instanceKey)
	}

	log.Infof("Will attempt to enable GTID on %+v", *instanceKey)

	instance, err = Repoint(instanceKey, nil, GTIDHintForce)
	if err != nil {
		return instance, err
	}
	if !instance.UsingGTID() {
		return instance, fmt.Errorf("Cannot enable GTID on %+v", *instanceKey)
	}

	AuditOperation("enable-gtid", instanceKey, fmt.Sprintf("enabled GTID on %+v", *instanceKey))

	return instance, err
}

func DisableGTID(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	if !instance.UsingGTID() {
		return instance, fmt.Errorf("%+v is not using GTID", *instanceKey)
	}

	log.Infof("Will attempt to disable GTID on %+v", *instanceKey)

	instance, err = Repoint(instanceKey, nil, GTIDHintDeny)
	if err != nil {
		return instance, err
	}
	if instance.UsingGTID() {
		return instance, fmt.Errorf("Cannot disable GTID on %+v", *instanceKey)
	}

	AuditOperation("disable-gtid", instanceKey, fmt.Sprintf("disabled GTID on %+v", *instanceKey))

	return instance, err
}

func ResetMasterGTIDOperation(instanceKey *InstanceKey, removeSelfUUID bool, uuidToRemove string) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	if !instance.UsingOracleGTID {
		return instance, log.Errorf("reset-master-gtid requested for %+v but it is not using oracle-gtid", *instanceKey)
	}
	if len(instance.SlaveHosts) > 0 {
		return instance, log.Errorf("reset-master-gtid will not operate on %+v because it has %+v slaves. Expecting no slaves", *instanceKey, len(instance.SlaveHosts))
	}

	log.Infof("Will reset master on %+v", instanceKey)

	var oracleGtidSet *OracleGtidSet
	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "reset-master-gtid"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", *instanceKey)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	if instance.IsSlave() {
		instance, err = StopSlave(instanceKey)
		if err != nil {
			goto Cleanup
		}
	}

	oracleGtidSet, err = ParseGtidSet(instance.ExecutedGtidSet)
	if err != nil {
		goto Cleanup
	}
	if removeSelfUUID {
		uuidToRemove = instance.ServerUUID
	}
	if uuidToRemove != "" {
		removed := oracleGtidSet.RemoveUUID(uuidToRemove)
		if removed {
			log.Debugf("Will remove UUID %s", uuidToRemove)
		} else {
			log.Debugf("UUID %s not found", uuidToRemove)
		}
	}

	instance, err = ResetMaster(instanceKey)
	if err != nil {
		goto Cleanup
	}
	err = setGTIDPurged(instance, oracleGtidSet.String())
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartSlave(instanceKey)

	if err != nil {
		return instance, log.Errore(err)
	}

	AuditOperation("reset-master-gtid", instanceKey, fmt.Sprintf("%+v master reset", *instanceKey))

	return instance, err
}

func FindLastPseudoGTIDEntry(instance *Instance, recordedInstanceRelayLogCoordinates BinlogCoordinates, maxBinlogCoordinates *BinlogCoordinates, exhaustiveSearch bool, expectedBinlogFormat *string) (instancePseudoGtidCoordinates *BinlogCoordinates, instancePseudoGtidText string, err error) {

	if config.Config.PseudoGTIDPattern == "" {
		return instancePseudoGtidCoordinates, instancePseudoGtidText, fmt.Errorf("PseudoGTIDPattern not configured; cannot use Pseudo-GTID")
	}

	minBinlogCoordinates, minRelaylogCoordinates, err := GetHeuristiclyRecentCoordinatesForInstance(&instance.Key)
	if instance.LogBinEnabled && instance.LogSlaveUpdatesEnabled && (expectedBinlogFormat == nil || instance.Binlog_format == *expectedBinlogFormat) {
		instancePseudoGtidCoordinates, instancePseudoGtidText, err = getLastPseudoGTIDEntryInInstance(instance, minBinlogCoordinates, maxBinlogCoordinates, exhaustiveSearch)
	}
	if err != nil || instancePseudoGtidCoordinates == nil {
		instancePseudoGtidCoordinates, instancePseudoGtidText, err = getLastPseudoGTIDEntryInRelayLogs(instance, minRelaylogCoordinates, recordedInstanceRelayLogCoordinates, exhaustiveSearch)
	}
	return instancePseudoGtidCoordinates, instancePseudoGtidText, err
}

func CorrelateBinlogCoordinates(instance *Instance, binlogCoordinates *BinlogCoordinates, otherInstance *Instance) (*BinlogCoordinates, int, error) {
	recordedInstanceRelayLogCoordinates := instance.RelaylogCoordinates
	instancePseudoGtidCoordinates, instancePseudoGtidText, err := FindLastPseudoGTIDEntry(instance, recordedInstanceRelayLogCoordinates, binlogCoordinates, true, &otherInstance.Binlog_format)

	if err != nil {
		return nil, 0, err
	}
	entriesMonotonic := (config.Config.PseudoGTIDMonotonicHint != "") && strings.Contains(instancePseudoGtidText, config.Config.PseudoGTIDMonotonicHint)
	minBinlogCoordinates, _, err := GetHeuristiclyRecentCoordinatesForInstance(&otherInstance.Key)
	otherInstancePseudoGtidCoordinates, err := SearchEntryInInstanceBinlogs(otherInstance, instancePseudoGtidText, entriesMonotonic, minBinlogCoordinates)
	if err != nil {
		return nil, 0, err
	}

	nextBinlogCoordinatesToMatch, countMatchedEvents, err := GetNextBinlogCoordinatesToMatch(instance, *instancePseudoGtidCoordinates,
		recordedInstanceRelayLogCoordinates, binlogCoordinates, otherInstance, *otherInstancePseudoGtidCoordinates)
	if err != nil {
		return nil, 0, err
	}
	if countMatchedEvents == 0 {
		err = fmt.Errorf("Unexpected: 0 events processed while iterating logs. Something went wrong; aborting. nextBinlogCoordinatesToMatch: %+v", nextBinlogCoordinatesToMatch)
		return nil, 0, err
	}
	return nextBinlogCoordinatesToMatch, countMatchedEvents, nil
}

func MatchBelow(instanceKey, otherKey *InstanceKey, requireInstanceMaintenance bool) (*Instance, *BinlogCoordinates, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, nil, err
	}
	if config.Config.PseudoGTIDPattern == "" {
		return instance, nil, fmt.Errorf("PseudoGTIDPattern not configured; cannot use Pseudo-GTID")
	}
	if instanceKey.Equals(otherKey) {
		return instance, nil, fmt.Errorf("MatchBelow: attempt to match an instance below itself %+v", *instanceKey)
	}
	otherInstance, err := ReadTopologyInstance(otherKey)
	if err != nil {
		return instance, nil, err
	}

	rinstance, _, _ := ReadInstance(&instance.Key)
	if canMove, merr := rinstance.CanMoveViaMatch(); !canMove {
		return instance, nil, merr
	}

	if canReplicate, err := instance.CanReplicateFrom(otherInstance); !canReplicate {
		return instance, nil, err
	}
	var nextBinlogCoordinatesToMatch *BinlogCoordinates
	var countMatchedEvents int

	if otherInstance.IsBinlogServer() {
		err = fmt.Errorf("Cannot use PseudoGTID with Binlog Server %+v", otherInstance.Key)
		goto Cleanup
	}

	log.Infof("Will match %+v below %+v", *instanceKey, *otherKey)

	if requireInstanceMaintenance {
		if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), fmt.Sprintf("match below %+v", *otherKey)); merr != nil {
			err = fmt.Errorf("Cannot begin maintenance on %+v", *instanceKey)
			goto Cleanup
		} else {
			defer EndMaintenance(maintenanceToken)
		}
	}

	log.Debugf("Stopping slave on %+v", *instanceKey)
	instance, err = StopSlave(instanceKey)
	if err != nil {
		goto Cleanup
	}

	nextBinlogCoordinatesToMatch, countMatchedEvents, err = CorrelateBinlogCoordinates(instance, nil, otherInstance)

	if countMatchedEvents == 0 {
		err = fmt.Errorf("Unexpected: 0 events processed while iterating logs. Something went wrong; aborting. nextBinlogCoordinatesToMatch: %+v", nextBinlogCoordinatesToMatch)
		goto Cleanup
	}
	log.Debugf("%+v will match below %+v at %+v; validated events: %d", *instanceKey, *otherKey, *nextBinlogCoordinatesToMatch, countMatchedEvents)

	instance, err = ChangeMasterTo(instanceKey, otherKey, nextBinlogCoordinatesToMatch, false, GTIDHintDeny)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartSlave(instanceKey)
	if err != nil {
		return instance, nextBinlogCoordinatesToMatch, log.Errore(err)
	}
	AuditOperation("match-below", instanceKey, fmt.Sprintf("matched %+v below %+v", *instanceKey, *otherKey))

	return instance, nextBinlogCoordinatesToMatch, err
}

func RematchSlave(instanceKey *InstanceKey, requireInstanceMaintenance bool) (*Instance, *BinlogCoordinates, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, nil, err
	}
	masterInstance, found, err := ReadInstance(&instance.MasterKey)
	if err != nil || !found {
		return instance, nil, err
	}
	return MatchBelow(instanceKey, &masterInstance.Key, requireInstanceMaintenance)
}

func MakeMaster(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	masterInstance, err := ReadTopologyInstance(&instance.MasterKey)
	if err != nil {
		if masterInstance.IsSlave() {
			return instance, fmt.Errorf("MakeMaster: instance's master %+v seems to be replicating", masterInstance.Key)
		}
		if masterInstance.IsLastCheckValid {
			return instance, fmt.Errorf("MakeMaster: instance's master %+v seems to be accessible", masterInstance.Key)
		}
	}
	if !instance.SQLThreadUpToDate() {
		return instance, fmt.Errorf("MakeMaster: instance's SQL thread must be up-to-date with I/O thread for %+v", *instanceKey)
	}
	siblings, err := ReadSlaveInstances(&masterInstance.Key)
	if err != nil {
		return instance, err
	}
	for _, sibling := range siblings {
		if instance.ExecBinlogCoordinates.SmallerThan(&sibling.ExecBinlogCoordinates) {
			return instance, fmt.Errorf("MakeMaster: instance %+v has more advanced sibling: %+v", *instanceKey, sibling.Key)
		}
	}

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), fmt.Sprintf("siblings match below this: %+v", *instanceKey)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", *instanceKey)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	_, _, err, _ = MultiMatchBelow(siblings, instanceKey, false, nil)
	if err != nil {
		goto Cleanup
	}

	SetReadOnly(instanceKey, false)

Cleanup:
	if err != nil {
		return instance, log.Errore(err)
	}
	AuditOperation("make-master", instanceKey, fmt.Sprintf("made master of %+v", *instanceKey))

	return instance, err
}

func EnslaveSiblings(instanceKey *InstanceKey) (*Instance, int, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, 0, err
	}
	masterInstance, found, err := ReadInstance(&instance.MasterKey)
	if err != nil || !found {
		return instance, 0, err
	}
	siblings, err := ReadSlaveInstances(&masterInstance.Key)
	if err != nil {
		return instance, 0, err
	}
	enslavedSiblings := 0
	for _, sibling := range siblings {
		if _, err := MoveBelow(&sibling.Key, &instance.Key); err == nil {
			enslavedSiblings++
		}
	}

	return instance, enslavedSiblings, err
}

func EnslaveMaster(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	masterInstance, found, err := ReadInstance(&instance.MasterKey)
	if err != nil || !found {
		return instance, err
	}
	log.Debugf("EnslaveMaster: will attempt making %+v enslave its master %+v, now resolved as %+v", *instanceKey, instance.MasterKey, masterInstance.Key)

	if canReplicate, err := masterInstance.CanReplicateFrom(instance); canReplicate == false {
		return instance, err
	}
	masterInstance, err = StopSlave(&masterInstance.Key)
	if err != nil {
		goto Cleanup
	}
	instance, err = StopSlave(&instance.Key)
	if err != nil {
		goto Cleanup
	}

	instance, err = StartSlaveUntilMasterCoordinates(&instance.Key, &masterInstance.SelfBinlogCoordinates)
	if err != nil {
		goto Cleanup
	}

	instance, err = ChangeMasterTo(&instance.Key, &masterInstance.MasterKey, &masterInstance.ExecBinlogCoordinates, true, GTIDHintNeutral)
	if err != nil {
		goto Cleanup
	}
	masterInstance, err = ChangeMasterTo(&masterInstance.Key, &instance.Key, &instance.SelfBinlogCoordinates, false, GTIDHintNeutral)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartSlave(&instance.Key)
	masterInstance, _ = StartSlave(&masterInstance.Key)
	if err != nil {
		return instance, err
	}
	AuditOperation("enslave-master", instanceKey, fmt.Sprintf("enslaved master: %+v", masterInstance.Key))

	return instance, err
}

func MakeLocalMaster(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, err
	}
	masterInstance, found, err := ReadInstance(&instance.MasterKey)
	if err != nil || !found {
		return instance, err
	}
	grandparentInstance, err := ReadTopologyInstance(&masterInstance.MasterKey)
	if err != nil {
		return instance, err
	}
	siblings, err := ReadSlaveInstances(&masterInstance.Key)
	if err != nil {
		return instance, err
	}
	for _, sibling := range siblings {
		if instance.ExecBinlogCoordinates.SmallerThan(&sibling.ExecBinlogCoordinates) {
			return instance, fmt.Errorf("MakeMaster: instance %+v has more advanced sibling: %+v", *instanceKey, sibling.Key)
		}
	}

	instance, err = StopSlaveNicely(instanceKey, 0)
	if err != nil {
		goto Cleanup
	}

	_, _, err = MatchBelow(instanceKey, &grandparentInstance.Key, true)
	if err != nil {
		goto Cleanup
	}

	_, _, err, _ = MultiMatchBelow(siblings, instanceKey, false, nil)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	if err != nil {
		return instance, log.Errore(err)
	}
	AuditOperation("make-local-master", instanceKey, fmt.Sprintf("made master of %+v", *instanceKey))

	return instance, err
}

func sortedSlaves(masterKey *InstanceKey, shouldStopSlaves bool, includeBinlogServerSubSlaves bool) (slaves [](*Instance), err error) {
	if includeBinlogServerSubSlaves {
		slaves, err = ReadSlaveInstancesIncludingBinlogServerSubSlaves(masterKey)
	} else {
		slaves, err = ReadSlaveInstances(masterKey)
	}
	if err != nil {
		return slaves, err
	}
	if len(slaves) == 0 {
		return slaves, nil
	}
	if shouldStopSlaves {
		log.Debugf("sortedSlaves: stopping %d slaves nicely", len(slaves))
		slaves = StopSlavesNicely(slaves, time.Duration(config.Config.InstanceBulkOperationsWaitTimeoutSeconds)*time.Second)
	}
	slaves = RemoveNilInstances(slaves)

	sort.Sort(sort.Reverse(InstancesByExecBinlogCoordinates(slaves)))
	for _, slave := range slaves {
		log.Debugf("- sorted slave: %+v %+v", slave.Key, slave.ExecBinlogCoordinates)
	}

	return slaves, err
}

func MultiMatchBelow(slaves [](*Instance), belowKey *InstanceKey, slavesAlreadyStopped bool, postponedFunctionsContainer *PostponedFunctionsContainer) ([](*Instance), *Instance, error, []error) {
	res := [](*Instance){}
	errs := []error{}
	slaveMutex := make(chan bool, 1)

	if config.Config.PseudoGTIDPattern == "" {
		return res, nil, fmt.Errorf("PseudoGTIDPattern not configured; cannot use Pseudo-GTID"), errs
	}

	slaves = RemoveInstance(slaves, belowKey)
	slaves = RemoveBinlogServerInstances(slaves)

	for _, slave := range slaves {
		if maintenanceToken, merr := BeginMaintenance(&slave.Key, GetMaintenanceOwner(), fmt.Sprintf("%+v match below %+v as part of MultiMatchBelow", slave.Key, *belowKey)); merr != nil {
			errs = append(errs, fmt.Errorf("Cannot begin maintenance on %+v", slave.Key))
			slaves = RemoveInstance(slaves, &slave.Key)
		} else {
			defer EndMaintenance(maintenanceToken)
		}
	}

	belowInstance, err := ReadTopologyInstance(belowKey)
	if err != nil {
		return res, belowInstance, err, errs
	}
	if belowInstance.IsBinlogServer() {
		err = fmt.Errorf("Cannot use PseudoGTID with Binlog Server %+v", belowInstance.Key)
		return res, belowInstance, err, errs
	}

	if len(slaves) == 0 {
		return res, belowInstance, nil, errs
	}
	if !slavesAlreadyStopped {
		log.Debugf("MultiMatchBelow: stopping %d slaves nicely", len(slaves))
		slaves = StopSlavesNicely(slaves, time.Duration(config.Config.InstanceBulkOperationsWaitTimeoutSeconds)*time.Second)
	}
	slaves = RemoveNilInstances(slaves)
	sort.Sort(sort.Reverse(InstancesByExecBinlogCoordinates(slaves)))

	slaveBuckets := make(map[BinlogCoordinates][](*Instance))
	for _, slave := range slaves {
		slave := slave
		slaveBuckets[slave.ExecBinlogCoordinates] = append(slaveBuckets[slave.ExecBinlogCoordinates], slave)
	}
	log.Debugf("MultiMatchBelow: %d slaves merged into %d buckets", len(slaves), len(slaveBuckets))
	for bucket, bucketSlaves := range slaveBuckets {
		log.Debugf("+- bucket: %+v, %d slaves", bucket, len(bucketSlaves))
	}
	matchedSlaves := make(map[InstanceKey]bool)
	bucketsBarrier := make(chan *BinlogCoordinates)

	for execCoordinates, bucketSlaves := range slaveBuckets {
		execCoordinates := execCoordinates
		bucketSlaves := bucketSlaves
		var bucketMatchedCoordinates *BinlogCoordinates
		go func() {
			defer func() { bucketsBarrier <- &execCoordinates }()
			func() {
				for _, slave := range bucketSlaves {
					slave := slave
					var slaveErr error
					var matchedCoordinates *BinlogCoordinates
					log.Debugf("MultiMatchBelow: attempting slave %+v in bucket %+v", slave.Key, execCoordinates)
					matchFunc := func() error {
						ExecuteOnTopology(func() {
							_, matchedCoordinates, slaveErr = MatchBelow(&slave.Key, &belowInstance.Key, false)
						})
						return nil
					}
					if postponedFunctionsContainer != nil &&
						config.Config.PostponeSlaveRecoveryOnLagMinutes > 0 &&
						slave.SQLDelay > config.Config.PostponeSlaveRecoveryOnLagMinutes*60 &&
						len(bucketSlaves) == 1 {
						(*postponedFunctionsContainer).AddPostponedFunction(matchFunc)
						return
					}
					matchFunc()
					log.Debugf("MultiMatchBelow: match result: %+v, %+v", matchedCoordinates, slaveErr)

					if slaveErr == nil {
						func() {
							slaveMutex <- true
							defer func() { <-slaveMutex }()
							bucketMatchedCoordinates = matchedCoordinates
							matchedSlaves[slave.Key] = true
						}()
						log.Debugf("MultiMatchBelow: matched slave %+v in bucket %+v", slave.Key, execCoordinates)
						return
					}

					func() {
						slaveMutex <- true
						defer func() { <-slaveMutex }()
						errs = append(errs, slaveErr)
					}()
					log.Errore(slaveErr)
				}
			}()
			if bucketMatchedCoordinates == nil {
				log.Errorf("MultiMatchBelow: Cannot match up %d slaves since their bucket %+v is failed", len(bucketSlaves), execCoordinates)
				return
			}
			log.Debugf("MultiMatchBelow: bucket %+v coordinates are: %+v. Proceeding to match all bucket slaves", execCoordinates, *bucketMatchedCoordinates)
			func() {
				barrier := make(chan *InstanceKey)
				for _, slave := range bucketSlaves {
					slave := slave
					go func() {
						defer func() { barrier <- &slave.Key }()

						var err error
						if _, found := matchedSlaves[slave.Key]; found {
							return
						}
						log.Debugf("MultiMatchBelow: Will match up %+v to previously matched master coordinates %+v", slave.Key, *bucketMatchedCoordinates)
						slaveMatchSuccess := false
						ExecuteOnTopology(func() {
							if _, err = ChangeMasterTo(&slave.Key, &belowInstance.Key, bucketMatchedCoordinates, false, GTIDHintDeny); err == nil {
								StartSlave(&slave.Key)
								slaveMatchSuccess = true
							}
						})
						func() {
							slaveMutex <- true
							defer func() { <-slaveMutex }()
							if slaveMatchSuccess {
								matchedSlaves[slave.Key] = true
							} else {
								errs = append(errs, err)
								log.Errorf("MultiMatchBelow: Cannot match up %+v: error is %+v", slave.Key, err)
							}
						}()
					}()
				}
				for range bucketSlaves {
					<-barrier
				}
			}()
		}()
	}
	for range slaveBuckets {
		<-bucketsBarrier
	}

	for _, slave := range slaves {
		slave := slave
		if _, found := matchedSlaves[slave.Key]; found {
			res = append(res, slave)
		}
	}
	return res, belowInstance, err, errs
}

func MultiMatchSlaves(masterKey *InstanceKey, belowKey *InstanceKey, pattern string) ([](*Instance), *Instance, error, []error) {
	res := [](*Instance){}
	errs := []error{}

	belowInstance, err := ReadTopologyInstance(belowKey)
	if err != nil {
		return res, nil, err, errs
	}

	masterInstance, found, err := ReadInstance(masterKey)
	if err != nil || !found {
		return res, nil, err, errs
	}

	binlogCase := false
	if masterInstance.IsBinlogServer() && masterInstance.MasterKey.Equals(belowKey) {
		log.Debugf("MultiMatchSlaves: pointing slaves up from binlog server")
		binlogCase = true
	} else if belowInstance.IsBinlogServer() && belowInstance.MasterKey.Equals(masterKey) {
		log.Debugf("MultiMatchSlaves: pointing slaves down to binlog server")
		binlogCase = true
	} else if masterInstance.IsBinlogServer() && belowInstance.IsBinlogServer() && masterInstance.MasterKey.Equals(&belowInstance.MasterKey) {
		log.Debugf("MultiMatchSlaves: pointing slaves to binlong sibling")
		binlogCase = true
	}
	if binlogCase {
		slaves, err, errors := RepointSlavesTo(masterKey, pattern, belowKey)
		return slaves, masterInstance, err, errors
	}

	slaves, err := ReadSlaveInstancesIncludingBinlogServerSubSlaves(masterKey)
	if err != nil {
		return res, belowInstance, err, errs
	}
	slaves = filterInstancesByPattern(slaves, pattern)
	matchedSlaves, belowInstance, err, errs := MultiMatchBelow(slaves, &belowInstance.Key, false, nil)

	if len(matchedSlaves) != len(slaves) {
		err = fmt.Errorf("MultiMatchSlaves: only matched %d out of %d slaves of %+v; error is: %+v", len(matchedSlaves), len(slaves), *masterKey, err)
	}
	AuditOperation("multi-match-slaves", masterKey, fmt.Sprintf("matched %d slaves under %+v", len(matchedSlaves), *belowKey))

	return matchedSlaves, belowInstance, err, errs
}

func MatchUp(instanceKey *InstanceKey, requireInstanceMaintenance bool) (*Instance, *BinlogCoordinates, error) {
	instance, found, err := ReadInstance(instanceKey)
	if err != nil || !found {
		return nil, nil, err
	}
	if !instance.IsSlave() {
		return instance, nil, fmt.Errorf("instance is not a slave: %+v", instanceKey)
	}
	master, found, err := ReadInstance(&instance.MasterKey)
	if err != nil || !found {
		return instance, nil, log.Errorf("Cannot get master for %+v. error=%+v", instance.Key, err)
	}

	if !master.IsSlave() {
		return instance, nil, fmt.Errorf("master is not a slave itself: %+v", master.Key)
	}

	return MatchBelow(instanceKey, &master.MasterKey, requireInstanceMaintenance)
}

func MatchUpSlaves(masterKey *InstanceKey, pattern string) ([](*Instance), *Instance, error, []error) {
	res := [](*Instance){}
	errs := []error{}

	masterInstance, found, err := ReadInstance(masterKey)
	if err != nil || !found {
		return res, nil, err, errs
	}

	return MultiMatchSlaves(masterKey, &masterInstance.MasterKey, pattern)
}

func isGenerallyValidAsBinlogSource(slave *Instance) bool {
	if !slave.IsLastCheckValid {
		return false
	}
	if !slave.LogBinEnabled {
		return false
	}
	if !slave.LogSlaveUpdatesEnabled {
		return false
	}

	return true
}

func isGenerallyValidAsCandidateSlave(slave *Instance) bool {
	if !isGenerallyValidAsBinlogSource(slave) {
		return false
	}
	if slave.IsBinlogServer() {
		return false
	}

	return true
}

func isValidAsCandidateMasterInBinlogServerTopology(slave *Instance) bool {
	if !slave.IsLastCheckValid {
		return false
	}
	if !slave.LogBinEnabled {
		return false
	}
	if slave.LogSlaveUpdatesEnabled {
		return false
	}
	if slave.IsBinlogServer() {
		return false
	}

	return true
}

func isBannedFromBeingCandidateSlave(slave *Instance) bool {
	if slave.PromotionRule == MustNotPromoteRule {
		log.Debugf("instance %+v is banned because of promotion rule", slave.Key)
		return true
	}
	for _, filter := range config.Config.PromotionIgnoreHostnameFilters {
		if matched, _ := regexp.MatchString(filter, slave.Key.Hostname); matched {
			return true
		}
	}
	return false
}

func GetCandidateSlave(masterKey *InstanceKey, forRematchPurposes bool) (*Instance, [](*Instance), [](*Instance), [](*Instance), error) {
	var candidateSlave *Instance
	aheadSlaves := [](*Instance){}
	equalSlaves := [](*Instance){}
	laterSlaves := [](*Instance){}

	slaves, err := sortedSlaves(masterKey, forRematchPurposes, false)
	if err != nil {
		return candidateSlave, aheadSlaves, equalSlaves, laterSlaves, err
	}
	if len(slaves) == 0 {
		return candidateSlave, aheadSlaves, equalSlaves, laterSlaves, fmt.Errorf("No slaves found for %+v", *masterKey)
	}
	for _, slave := range slaves {
		slave := slave
		if isGenerallyValidAsCandidateSlave(slave) && !isBannedFromBeingCandidateSlave(slave) {
			candidateSlave = slave
			break
		}
	}
	if candidateSlave == nil {
		for _, slave := range slaves {
			slave := slave
			if !isBannedFromBeingCandidateSlave(slave) {
				candidateSlave = slave
				break
			}
		}
		if candidateSlave != nil {
			slaves = RemoveInstance(slaves, &candidateSlave.Key)
		}

		return candidateSlave, slaves, equalSlaves, laterSlaves, fmt.Errorf("GetCandidateSlave: no candidate slaves found %+v", *masterKey)
	}
	slaves = RemoveInstance(slaves, &candidateSlave.Key)
	for _, slave := range slaves {
		slave := slave
		if slave.ExecBinlogCoordinates.SmallerThan(&candidateSlave.ExecBinlogCoordinates) {
			laterSlaves = append(laterSlaves, slave)
		} else if slave.ExecBinlogCoordinates.Equals(&candidateSlave.ExecBinlogCoordinates) {
			equalSlaves = append(equalSlaves, slave)
		} else {
			aheadSlaves = append(aheadSlaves, slave)
		}
	}
	log.Debugf("sortedSlaves: candidate: %+v, ahead: %d, equal: %d, late: %d", candidateSlave.Key, len(aheadSlaves), len(equalSlaves), len(laterSlaves))
	return candidateSlave, aheadSlaves, equalSlaves, laterSlaves, nil
}

func GetCandidateSlaveOfBinlogServerTopology(masterKey *InstanceKey) (candidateSlave *Instance, err error) {
	slaves, err := sortedSlaves(masterKey, false, true)
	if err != nil {
		return candidateSlave, err
	}
	if len(slaves) == 0 {
		return candidateSlave, fmt.Errorf("No slaves found for %+v", *masterKey)
	}
	for _, slave := range slaves {
		slave := slave
		if candidateSlave != nil {
			break
		}
		if isValidAsCandidateMasterInBinlogServerTopology(slave) && !isBannedFromBeingCandidateSlave(slave) {
			candidateSlave = slave
		}
	}
	if candidateSlave != nil {
		log.Debugf("GetCandidateSlaveOfBinlogServerTopology: returning %+v as candidate slave for %+v", candidateSlave.Key, *masterKey)
	} else {
		log.Debugf("GetCandidateSlaveOfBinlogServerTopology: no candidate slave found for %+v", *masterKey)
	}
	return candidateSlave, err
}

func RegroupSlavesPseudoGTID(masterKey *InstanceKey, returnSlaveEvenOnFailureToRegroup bool, onCandidateSlaveChosen func(*Instance), postponedFunctionsContainer *PostponedFunctionsContainer) ([](*Instance), [](*Instance), [](*Instance), *Instance, error) {
	candidateSlave, aheadSlaves, equalSlaves, laterSlaves, err := GetCandidateSlave(masterKey, true)
	if err != nil {
		if !returnSlaveEvenOnFailureToRegroup {
			candidateSlave = nil
		}
		return aheadSlaves, equalSlaves, laterSlaves, candidateSlave, err
	}

	if config.Config.PseudoGTIDPattern == "" {
		return aheadSlaves, equalSlaves, laterSlaves, candidateSlave, fmt.Errorf("PseudoGTIDPattern not configured; cannot use Pseudo-GTID")
	}

	if onCandidateSlaveChosen != nil {
		onCandidateSlaveChosen(candidateSlave)
	}

	log.Debugf("RegroupSlaves: working on %d equals slaves", len(equalSlaves))
	barrier := make(chan *InstanceKey)
	for _, slave := range equalSlaves {
		slave := slave
		go func() {
			defer func() { barrier <- &candidateSlave.Key }()
			ExecuteOnTopology(func() {
				ChangeMasterTo(&slave.Key, &candidateSlave.Key, &candidateSlave.SelfBinlogCoordinates, false, GTIDHintDeny)
			})
		}()
	}
	for range equalSlaves {
		<-barrier
	}

	log.Debugf("RegroupSlaves: multi matching %d later slaves", len(laterSlaves))
	laterSlaves, instance, err, _ := MultiMatchBelow(laterSlaves, &candidateSlave.Key, true, postponedFunctionsContainer)

	operatedSlaves := append(equalSlaves, candidateSlave)
	operatedSlaves = append(operatedSlaves, laterSlaves...)
	log.Debugf("RegroupSlaves: starting %d slaves", len(operatedSlaves))
	barrier = make(chan *InstanceKey)
	for _, slave := range operatedSlaves {
		slave := slave
		go func() {
			defer func() { barrier <- &candidateSlave.Key }()
			ExecuteOnTopology(func() {
				StartSlave(&slave.Key)
			})
		}()
	}
	for range operatedSlaves {
		<-barrier
	}

	log.Debugf("RegroupSlaves: done")
	AuditOperation("regroup-slaves", masterKey, fmt.Sprintf("regrouped %+v slaves below %+v", len(operatedSlaves), *masterKey))
	return aheadSlaves, equalSlaves, laterSlaves, instance, err
}

func getMostUpToDateActiveBinlogServer(masterKey *InstanceKey) (mostAdvancedBinlogServer *Instance, binlogServerSlaves [](*Instance), err error) {
	if binlogServerSlaves, err = ReadBinlogServerSlaveInstances(masterKey); err == nil && len(binlogServerSlaves) > 0 {
		for _, binlogServer := range binlogServerSlaves {
			if binlogServer.IsLastCheckValid {
				if mostAdvancedBinlogServer == nil {
					mostAdvancedBinlogServer = binlogServer
				}
				if mostAdvancedBinlogServer.ExecBinlogCoordinates.SmallerThan(&binlogServer.ExecBinlogCoordinates) {
					mostAdvancedBinlogServer = binlogServer
				}
			}
		}
	}
	return mostAdvancedBinlogServer, binlogServerSlaves, err
}

func RegroupSlavesPseudoGTIDIncludingSubSlavesOfBinlogServers(masterKey *InstanceKey, returnSlaveEvenOnFailureToRegroup bool, onCandidateSlaveChosen func(*Instance), postponedFunctionsContainer *PostponedFunctionsContainer) ([](*Instance), [](*Instance), [](*Instance), *Instance, error) {
	func() error {
		log.Debugf("RegroupSlavesIncludingSubSlavesOfBinlogServers: starting on slaves of %+v", *masterKey)
		mostUpToDateBinlogServer, binlogServerSlaves, err := getMostUpToDateActiveBinlogServer(masterKey)
		if err != nil {
			return log.Errore(err)
		}
		if mostUpToDateBinlogServer == nil {
			log.Debugf("RegroupSlavesIncludingSubSlavesOfBinlogServers: no binlog server replicates from %+v", *masterKey)
			return nil
		}
		log.Debugf("RegroupSlavesIncludingSubSlavesOfBinlogServers: most up to date binlog server of %+v: %+v", *masterKey, mostUpToDateBinlogServer.Key)

		candidateSlave, _, _, _, err := GetCandidateSlave(masterKey, true)
		if err != nil {
			return log.Errore(err)
		}
		if candidateSlave == nil {
			log.Debugf("RegroupSlavesIncludingSubSlavesOfBinlogServers: no candidate slave for %+v", *masterKey)
			return nil
		}
		log.Debugf("RegroupSlavesIncludingSubSlavesOfBinlogServers: candidate slave of %+v: %+v", *masterKey, candidateSlave.Key)

		if candidateSlave.ExecBinlogCoordinates.SmallerThan(&mostUpToDateBinlogServer.ExecBinlogCoordinates) {
			log.Debugf("RegroupSlavesIncludingSubSlavesOfBinlogServers: candidate slave %+v coordinates smaller than binlog server %+v", candidateSlave.Key, mostUpToDateBinlogServer.Key)
			candidateSlave, err = Repoint(&candidateSlave.Key, &mostUpToDateBinlogServer.Key, GTIDHintDeny)
			if err != nil {
				return log.Errore(err)
			}
			log.Debugf("RegroupSlavesIncludingSubSlavesOfBinlogServers: repointed candidate slave %+v under binlog server %+v", candidateSlave.Key, mostUpToDateBinlogServer.Key)
			candidateSlave, err = StartSlaveUntilMasterCoordinates(&candidateSlave.Key, &mostUpToDateBinlogServer.ExecBinlogCoordinates)
			if err != nil {
				return log.Errore(err)
			}
			log.Debugf("RegroupSlavesIncludingSubSlavesOfBinlogServers: aligned candidate slave %+v under binlog server %+v", candidateSlave.Key, mostUpToDateBinlogServer.Key)
			candidateSlave, err = Repoint(&candidateSlave.Key, masterKey, GTIDHintDeny)
			if err != nil {
				return log.Errore(err)
			}
			log.Debugf("RegroupSlavesIncludingSubSlavesOfBinlogServers: repointed candidate slave %+v under master %+v", candidateSlave.Key, *masterKey)
			return nil
		}
		for _, binlogServer := range binlogServerSlaves {
			log.Debugf("RegroupSlavesIncludingSubSlavesOfBinlogServers: matching slaves of binlog server %+v below %+v", binlogServer.Key, candidateSlave.Key)
			MultiMatchSlaves(&binlogServer.Key, &candidateSlave.Key, "")
			log.Debugf("RegroupSlavesIncludingSubSlavesOfBinlogServers: done matching slaves of binlog server %+v below %+v", binlogServer.Key, candidateSlave.Key)
		}
		log.Debugf("RegroupSlavesIncludingSubSlavesOfBinlogServers: done handling binlog regrouping for %+v; will proceed with normal RegroupSlaves", *masterKey)
		AuditOperation("regroup-slaves-including-bls", masterKey, fmt.Sprintf("matched slaves of binlog server slaves of %+v under %+v", *masterKey, candidateSlave.Key))
		return nil
	}()
	return RegroupSlavesPseudoGTID(masterKey, returnSlaveEvenOnFailureToRegroup, onCandidateSlaveChosen, postponedFunctionsContainer)
}

func RegroupSlavesGTID(masterKey *InstanceKey, returnSlaveEvenOnFailureToRegroup bool, onCandidateSlaveChosen func(*Instance)) ([](*Instance), [](*Instance), *Instance, error) {
	var emptySlaves [](*Instance)
	candidateSlave, aheadSlaves, equalSlaves, laterSlaves, err := GetCandidateSlave(masterKey, true)
	if err != nil {
		if !returnSlaveEvenOnFailureToRegroup {
			candidateSlave = nil
		}
		return emptySlaves, emptySlaves, candidateSlave, err
	}

	if onCandidateSlaveChosen != nil {
		onCandidateSlaveChosen(candidateSlave)
	}

	slavesToMove := append(equalSlaves, laterSlaves...)
	log.Debugf("RegroupSlavesGTID: working on %d slaves", len(slavesToMove))

	movedSlaves, unmovedSlaves, err, _ := moveSlavesViaGTID(slavesToMove, candidateSlave)
	if err != nil {
		log.Errore(err)
	}
	unmovedSlaves = append(unmovedSlaves, aheadSlaves...)
	StartSlave(&candidateSlave.Key)

	log.Debugf("RegroupSlavesGTID: done")
	AuditOperation("regroup-slaves-gtid", masterKey, fmt.Sprintf("regrouped slaves of %+v via GTID; promoted %+v", *masterKey, candidateSlave.Key))
	return unmovedSlaves, movedSlaves, candidateSlave, err
}

func RegroupSlavesBinlogServers(masterKey *InstanceKey, returnSlaveEvenOnFailureToRegroup bool) (repointedBinlogServers [](*Instance), promotedBinlogServer *Instance, err error) {
	var binlogServerSlaves [](*Instance)
	promotedBinlogServer, binlogServerSlaves, err = getMostUpToDateActiveBinlogServer(masterKey)

	resultOnError := func(err error) ([](*Instance), *Instance, error) {
		if !returnSlaveEvenOnFailureToRegroup {
			promotedBinlogServer = nil
		}
		return repointedBinlogServers, promotedBinlogServer, err
	}

	if err != nil {
		return resultOnError(err)
	}

	repointedBinlogServers, err, _ = RepointTo(binlogServerSlaves, &promotedBinlogServer.Key)

	if err != nil {
		return resultOnError(err)
	}
	AuditOperation("regroup-slaves-bls", masterKey, fmt.Sprintf("regrouped binlog server slaves of %+v; promoted %+v", *masterKey, promotedBinlogServer.Key))
	return repointedBinlogServers, promotedBinlogServer, nil
}

func RegroupSlaves(masterKey *InstanceKey, returnSlaveEvenOnFailureToRegroup bool,
	onCandidateSlaveChosen func(*Instance),
	postponedFunctionsContainer *PostponedFunctionsContainer) (
	aheadSlaves [](*Instance), equalSlaves [](*Instance), laterSlaves [](*Instance), instance *Instance, err error) {
	var emptySlaves [](*Instance)

	slaves, err := ReadSlaveInstances(masterKey)
	if err != nil {
		return emptySlaves, emptySlaves, emptySlaves, instance, err
	}
	if len(slaves) == 0 {
		return emptySlaves, emptySlaves, emptySlaves, instance, err
	}
	if len(slaves) == 1 {
		return emptySlaves, emptySlaves, emptySlaves, slaves[0], err
	}
	allGTID := true
	allBinlogServers := true
	allPseudoGTID := true
	for _, slave := range slaves {
		if !slave.UsingGTID() {
			allGTID = false
		}
		if !slave.IsBinlogServer() {
			allBinlogServers = false
		}
		if !slave.UsingPseudoGTID {
			allPseudoGTID = false
		}
	}
	if allGTID {
		log.Debugf("RegroupSlaves: using GTID to regroup slaves of %+v", *masterKey)
		unmovedSlaves, movedSlaves, candidateSlave, err := RegroupSlavesGTID(masterKey, returnSlaveEvenOnFailureToRegroup, onCandidateSlaveChosen)
		return unmovedSlaves, emptySlaves, movedSlaves, candidateSlave, err
	}
	if allBinlogServers {
		log.Debugf("RegroupSlaves: using binlog servers to regroup slaves of %+v", *masterKey)
		movedSlaves, candidateSlave, err := RegroupSlavesBinlogServers(masterKey, returnSlaveEvenOnFailureToRegroup)
		return emptySlaves, emptySlaves, movedSlaves, candidateSlave, err
	}
	if allPseudoGTID {
		log.Debugf("RegroupSlaves: using Pseudo-GTID to regroup slaves of %+v", *masterKey)
		return RegroupSlavesPseudoGTID(masterKey, returnSlaveEvenOnFailureToRegroup, onCandidateSlaveChosen, postponedFunctionsContainer)
	}
	log.Warningf("RegroupSlaves: unsure what method to invoke for %+v; trying Pseudo-GTID+Binlog Servers", *masterKey)
	return RegroupSlavesPseudoGTIDIncludingSubSlavesOfBinlogServers(masterKey, returnSlaveEvenOnFailureToRegroup, onCandidateSlaveChosen, postponedFunctionsContainer)
}

func relocateBelowInternal(instance, other *Instance) (*Instance, error) {
	if canReplicate, err := instance.CanReplicateFrom(other); !canReplicate {
		return instance, log.Errorf("%+v cannot replicate from %+v. Reason: %+v", instance.Key, other.Key, err)
	}
	if InstanceIsMasterOf(other, instance) {
		return Repoint(&instance.Key, &other.Key, GTIDHintNeutral)
	}
	if !instance.IsBinlogServer() {
		if movedInstance, err := MoveEquivalent(&instance.Key, &other.Key); err == nil {
			return movedInstance, nil
		}
	}
	if InstancesAreSiblings(instance, other) && other.IsBinlogServer() {
		return MoveBelow(&instance.Key, &other.Key)
	}
	instanceMaster, found, err := ReadInstance(&instance.MasterKey)
	if err != nil || !found {
		return instance, err
	}
	if instanceMaster.MasterKey.Equals(&other.Key) && instanceMaster.IsBinlogServer() {
		return Repoint(&instance.Key, &instanceMaster.MasterKey, GTIDHintDeny)
	}
	if other.IsBinlogServer() {
		if instanceMaster.IsBinlogServer() && InstancesAreSiblings(instanceMaster, other) {
			return Repoint(&instance.Key, &other.Key, GTIDHintDeny)
		}

		otherMaster, found, err := ReadInstance(&other.MasterKey)
		if err != nil || !found {
			return instance, err
		}
		if !other.IsLastCheckValid {
			return instance, log.Errorf("Binlog server %+v is not reachable. It would take two steps to relocate %+v below it, and I won't even do the first step.", other.Key, instance.Key)
		}

		log.Debugf("Relocating to a binlog server; will first attempt to relocate to the binlog server's master: %+v, and then repoint down", otherMaster.Key)
		if _, err := relocateBelowInternal(instance, otherMaster); err != nil {
			return instance, err
		}
		return Repoint(&instance.Key, &other.Key, GTIDHintDeny)
	}
	if instance.IsBinlogServer() {
		return nil, log.Errorf("Relocating binlog server %+v below %+v turns to be too complex; please do it manually", instance.Key, other.Key)
	}
	if _, _, canMove := canMoveViaGTID(instance, other); canMove {
		return moveInstanceBelowViaGTID(instance, other)
	}

	if instance.UsingPseudoGTID && other.UsingPseudoGTID {
		instance, _, err := MatchBelow(&instance.Key, &other.Key, true)
		return instance, err
	}
	if InstancesAreSiblings(instance, other) {
		return MoveBelow(&instance.Key, &other.Key)
	}
	if instanceMaster.MasterKey.Equals(&other.Key) {
		return MoveUp(&instance.Key)
	}
	if instanceMaster.IsBinlogServer() {
		if _, err := MoveUp(&instance.Key); err != nil {
			return instance, err
		}
		return relocateBelowInternal(instance, other)
	}
	return nil, log.Errorf("Relocating %+v below %+v turns to be too complex; please do it manually", instance.Key, other.Key)
}

func RelocateBelow(instanceKey, otherKey *InstanceKey) (*Instance, error) {
	instance, found, err := ReadInstance(instanceKey)
	if err != nil || !found {
		return instance, log.Errorf("Error reading %+v", *instanceKey)
	}
	other, found, err := ReadInstance(otherKey)
	if err != nil || !found {
		return instance, log.Errorf("Error reading %+v", *otherKey)
	}
	instance, err = relocateBelowInternal(instance, other)
	if err == nil {
		AuditOperation("relocate-below", instanceKey, fmt.Sprintf("relocated %+v below %+v", *instanceKey, *otherKey))
	}
	return instance, err
}

func relocateSlavesInternal(slaves [](*Instance), instance, other *Instance) ([](*Instance), error, []error) {
	errs := []error{}
	var err error
	if instance.Key.Equals(&other.Key) {
		return RepointTo(slaves, &other.Key)
	}
	if InstanceIsMasterOf(other, instance) && instance.IsBinlogServer() {
		return RepointTo(slaves, &other.Key)
	}
	if InstanceIsMasterOf(instance, other) && other.IsBinlogServer() {
		return RepointTo(slaves, &other.Key)
	}
	if InstancesAreSiblings(instance, other) && instance.IsBinlogServer() && other.IsBinlogServer() {
		return RepointTo(slaves, &other.Key)
	}
	if other.IsBinlogServer() {
		otherMaster, found, err := ReadInstance(&other.MasterKey)
		if err != nil || !found {
			return nil, err, errs
		}
		slaves, err, errs = relocateSlavesInternal(slaves, instance, otherMaster)
		if err != nil {
			return slaves, err, errs
		}

		return RepointTo(slaves, &other.Key)
	}
	{
		movedSlaves, unmovedSlaves, err, errs := moveSlavesViaGTID(slaves, other)

		if len(movedSlaves) == len(slaves) {
			return movedSlaves, err, errs
		} else if len(movedSlaves) > 0 {
			return relocateSlavesInternal(unmovedSlaves, instance, other)
		}
	}

	if other.UsingPseudoGTID {
		var pseudoGTIDSlaves [](*Instance)
		for _, slave := range slaves {
			if slave.UsingPseudoGTID {
				pseudoGTIDSlaves = append(pseudoGTIDSlaves, slave)
			}
		}
		pseudoGTIDSlaves, _, err, errs = MultiMatchBelow(pseudoGTIDSlaves, &other.Key, false, nil)
		return pseudoGTIDSlaves, err, errs
	}

	if InstanceIsMasterOf(other, instance) {
		// MEEEHHHH
	}

	return nil, log.Errorf("Relocating %+v slaves of %+v below %+v turns to be too complex; please do it manually", len(slaves), instance.Key, other.Key), errs
}

func RelocateSlaves(instanceKey, otherKey *InstanceKey, pattern string) (slaves [](*Instance), other *Instance, err error, errs []error) {

	instance, found, err := ReadInstance(instanceKey)
	if err != nil || !found {
		return slaves, other, log.Errorf("Error reading %+v", *instanceKey), errs
	}
	other, found, err = ReadInstance(otherKey)
	if err != nil || !found {
		return slaves, other, log.Errorf("Error reading %+v", *otherKey), errs
	}

	slaves, err = ReadSlaveInstances(instanceKey)
	if err != nil {
		return slaves, other, err, errs
	}
	slaves = RemoveInstance(slaves, otherKey)
	slaves = filterInstancesByPattern(slaves, pattern)
	if len(slaves) == 0 {
		return slaves, other, nil, errs
	}
	slaves, err, errs = relocateSlavesInternal(slaves, instance, other)

	if err == nil {
		AuditOperation("relocate-slaves", instanceKey, fmt.Sprintf("relocated %+v slaves of %+v below %+v", len(slaves), *instanceKey, *otherKey))
	}
	return slaves, other, err, errs
}
