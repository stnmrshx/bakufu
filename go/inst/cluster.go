/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package inst

import (
	"github.com/stnmrshx/bakufu/go/config"
	"regexp"
	"strings"
)

type ClusterInfo struct {
	ClusterName                            string
	ClusterAlias                           string
	ClusterDomain                          string
	CountInstances                         uint
	HeuristicLag                           int64
	HasAutomatedMasterRecovery             bool
	HasAutomatedIntermediateMasterRecovery bool
}

func (this *ClusterInfo) ReadRecoveryInfo() {
	this.HasAutomatedMasterRecovery = this.filtersMatchCluster(config.Config.RecoverMasterClusterFilters)
	this.HasAutomatedIntermediateMasterRecovery = this.filtersMatchCluster(config.Config.RecoverIntermediateMasterClusterFilters)
}

func (this *ClusterInfo) filtersMatchCluster(filters []string) bool {
	for _, filter := range filters {
		if strings.HasPrefix(filter, "alias=") {
			alias := strings.SplitN(filter, "=", 2)[1]
			if alias == this.ClusterAlias {
				return true
			}
		} else if strings.HasPrefix(filter, "alias~=") {
			aliasPattern := strings.SplitN(filter, "~=", 2)[1]
			if matched, _ := regexp.MatchString(aliasPattern, this.ClusterAlias); matched {
				return true
			}
		} else if filter == "*" {
			return true
		} else if matched, _ := regexp.MatchString(filter, this.ClusterName); matched && filter != "" {
			return true
		}
	}
	return false
}
