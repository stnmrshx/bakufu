/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package inst

import (
	"fmt"
	"github.com/stnmrshx/bakufu/go/config"
	"regexp"
	"sync"
)

var clusterAliasMap = make(map[string]string)
var clusterAliasMapMutex = &sync.Mutex{}

func ApplyClusterAlias(clusterInfo *ClusterInfo) {
	for pattern := range config.Config.ClusterNameToAlias {
		if matched, _ := regexp.MatchString(pattern, clusterInfo.ClusterName); matched {
			clusterInfo.ClusterAlias = config.Config.ClusterNameToAlias[pattern]
		}
	}
	clusterAliasMapMutex.Lock()
	defer clusterAliasMapMutex.Unlock()
	if alias, ok := clusterAliasMap[clusterInfo.ClusterName]; ok {
		clusterInfo.ClusterAlias = alias
	}
}

func SetClusterAlias(clusterName string, alias string) error {
	err := WriteClusterAlias(clusterName, alias)
	if err != nil {
		return err
	}
	clusterAliasMapMutex.Lock()
	defer clusterAliasMapMutex.Unlock()
	clusterAliasMap[clusterName] = alias

	return nil
}

func GetClusterByAlias(alias string) (string, error) {
	clusterName := ""
	clusterAliasMapMutex.Lock()
	defer clusterAliasMapMutex.Unlock()

	for mappedName, mappedAlias := range clusterAliasMap {
		if mappedAlias == alias {
			if clusterName == "" {
				clusterName = mappedName
			} else {
				return clusterName, fmt.Errorf("GetClusterByAlias: multiple clusters for alias %s", alias)
			}
		}
	}
	if clusterName == "" {
		return "", fmt.Errorf("GetClusterByAlias: no cluster found for alias %s", alias)
	}
	return clusterName, nil
}
