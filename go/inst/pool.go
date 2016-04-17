/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package inst

import (
	"strings"
	"github.com/stnmrshx/bakufu/go/config"
	"github.com/stnmrshx/golib/log"
)

type PoolInstancesMap map[string]([]*InstanceKey)

type ClusterPoolInstance struct {
	ClusterName  string
	ClusterAlias string
	Pool         string
	Hostname     string
	Port         int
}

func ApplyPoolInstances(pool string, instancesList string) error {
	var instanceKeys [](*InstanceKey)
	if instancesList != "" {
		instancesStrings := strings.Split(instancesList, ",")
		for _, instanceString := range instancesStrings {

			instanceKey, err := ParseInstanceKeyLoose(instanceString)
			if config.Config.SupportFuzzyPoolHostnames {
				instanceKey = ReadFuzzyInstanceKeyIfPossible(instanceKey)
			}
			log.Debugf("%+v", instanceKey)
			if err != nil {
				return log.Errore(err)
			}

			instanceKeys = append(instanceKeys, instanceKey)
		}
	}
	writePoolInstances(pool, instanceKeys)
	return nil
}
