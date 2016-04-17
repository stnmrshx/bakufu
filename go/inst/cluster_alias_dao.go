/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package inst

import (
	"fmt"

	"github.com/stnmrshx/golib/log"
	"github.com/stnmrshx/golib/sqlutils"
	"github.com/stnmrshx/bakufu/go/db"
)

func ReadClusterAliases() error {
	updatedMap := make(map[string]string)
	query := `
		select
			cluster_name,
			alias
		from
			cluster_alias
		`
	err := db.QueryBakufuRowsMap(query, func(m sqlutils.RowMap) error {
		updatedMap[m.GetString("cluster_name")] = m.GetString("alias")
		return nil
	})
	if err != nil {
		log.Errore(err)
	}
	clusterAliasMapMutex.Lock()
	defer clusterAliasMapMutex.Unlock()
	clusterAliasMap = updatedMap
	return err

}

func ReadClusterByAlias(alias string) (string, error) {
	clusterName := ""
	query := `
		select
			cluster_name
		from
			cluster_alias
		where
			alias = ?
			or cluster_name = ?
		`
	err := db.QueryBakufu(query, sqlutils.Args(alias, alias), func(m sqlutils.RowMap) error {
		clusterName = m.GetString("cluster_name")
		return nil
	})
	if err != nil {
		return "", err
	}
	if clusterName == "" {
		err = fmt.Errorf("No cluster found for alias %s", alias)
	}
	return clusterName, err

}

func WriteClusterAlias(clusterName string, alias string) error {
	writeFunc := func() error {
		_, err := db.ExecBakufu(`
			replace into
					cluster_alias (cluster_name, alias)
				values
					(?, ?)
			`,
			clusterName,
			alias)
		if err != nil {
			return log.Errore(err)
		}

		return nil
	}
	return ExecDBWriteFunc(writeFunc)
}

func UpdateClusterAliases() error {
	writeFunc := func() error {
		_, err := db.ExecBakufu(`
			replace into
					cluster_alias (alias, cluster_name, last_registered)
				select
				    suggested_cluster_alias,
				    substring_index(group_concat(cluster_name order by cluster_name), ',', 1) as cluster_name,
				    NOW()
				  from
				    database_instance
				    left join database_instance_downtime using (hostname, port)
				  where
				    suggested_cluster_alias!=''
				    and not (
				      (hostname, port) in (select hostname, port from topology_recovery where start_active_period >= now() - interval 11111 day)
				      and (
				        database_instance_downtime.downtime_active IS NULL
				        or database_instance_downtime.end_timestamp < NOW()
					  ) is false
				    )
				  group by
				    suggested_cluster_alias
			`)
		if err == nil {
			err = ReadClusterAliases()
		}
		return log.Errore(err)
	}
	return ExecDBWriteFunc(writeFunc)
}
