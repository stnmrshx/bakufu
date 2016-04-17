/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package inst

import (
	"github.com/stnmrshx/bakufu/go/config"
)

type Maintenance struct {
	MaintenanceId  uint
	Key            InstanceKey
	BeginTimestamp string
	SecondsElapsed uint
	IsActive       bool
	Owner          string
	Reason         string
}

var maintenanceOwner string = ""

func GetMaintenanceOwner() string {
	if maintenanceOwner != "" {
		return maintenanceOwner
	}
	return config.Config.MaintenanceOwner
}

func SetMaintenanceOwner(owner string) {
	maintenanceOwner = owner
}
