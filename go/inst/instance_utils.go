/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package inst

import (
	"regexp"
	"strings"
)

type InstancesByExecBinlogCoordinates [](*Instance)

func (this InstancesByExecBinlogCoordinates) Len() int      { return len(this) }
func (this InstancesByExecBinlogCoordinates) Swap(i, j int) { this[i], this[j] = this[j], this[i] }
func (this InstancesByExecBinlogCoordinates) Less(i, j int) bool {
	if this[i] == nil {
		return false
	}
	if this[j] == nil {
		return true
	}
	if this[i].ExecBinlogCoordinates.Equals(&this[j].ExecBinlogCoordinates) {
		if this[j].LogSlaveUpdatesEnabled && !this[i].LogSlaveUpdatesEnabled {
			return true
		}
	}
	return this[i].ExecBinlogCoordinates.SmallerThan(&this[j].ExecBinlogCoordinates)
}

func filterInstancesByPattern(instances [](*Instance), pattern string) [](*Instance) {
	if pattern == "" {
		return instances
	}
	filtered := [](*Instance){}
	for _, instance := range instances {
		if matched, _ := regexp.MatchString(pattern, instance.Key.DisplayString()); matched {
			filtered = append(filtered, instance)
		}
	}
	return filtered
}

func RemoveInstance(instances [](*Instance), instanceKey *InstanceKey) [](*Instance) {
	if instanceKey == nil {
		return instances
	}
	for i := len(instances) - 1; i >= 0; i-- {
		if instances[i].Key.Equals(instanceKey) {
			instances = append(instances[:i], instances[i+1:]...)
		}
	}
	return instances
}

func RemoveBinlogServerInstances(instances [](*Instance)) [](*Instance) {
	for i := len(instances) - 1; i >= 0; i-- {
		if instances[i].IsBinlogServer() {
			instances = append(instances[:i], instances[i+1:]...)
		}
	}
	return instances
}

func RemoveNilInstances(instances [](*Instance)) [](*Instance) {
	for i := len(instances) - 1; i >= 0; i-- {
		if instances[i] == nil {
			instances = append(instances[:i], instances[i+1:]...)
		}
	}
	return instances
}

func SemicolonTerminated(statement string) string {
	statement = strings.TrimSpace(statement)
	statement = strings.TrimRight(statement, ";")
	statement = statement + ";"
	return statement
}
