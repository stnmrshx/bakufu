/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package agent

import "github.com/stnmrshx/bakufu/go/inst"

type LogicalVolume struct {
	Name            string
	GroupName       string
	Path            string
	IsSnapshot      bool
	SnapshotPercent float64
}

type Mount struct {
	Path           string
	Device         string
	LVPath         string
	FileSystem     string
	IsMounted      bool
	DiskUsage      int64
	MySQLDataPath  string
	MySQLDiskUsage int64
}

type Agent struct {
	Hostname                string
	Port                    int
	Token                   string
	LastSubmitted           string
	AvailableLocalSnapshots []string
	AvailableSnapshots      []string
	LogicalVolumes          []LogicalVolume
	MountPoint              Mount
	MySQLRunning            bool
	MySQLDiskUsage          int64
	MySQLPort               int64
	MySQLDatadirDiskFree    int64
	MySQLErrorLogTail       []string
}

type SeedOperation struct {
	SeedId         int64
	TargetHostname string
	SourceHostname string
	StartTimestamp string
	EndTimestamp   string
	IsComplete     bool
	IsSuccessful   bool
}

type SeedOperationState struct {
	SeedStateId    int64
	SeedId         int64
	StateTimestamp string
	Action         string
	ErrorMessage   string
}

func (this *Agent) GetInstance() *inst.InstanceKey {
	return &inst.InstanceKey{Hostname: this.Hostname, Port: int(this.MySQLPort)}
}
