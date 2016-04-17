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

func writePoolInstances(pool string, instanceKeys [](*InstanceKey)) error {
	writeFunc := func() error {
		db, err := db.OpenBakufu()
		if err != nil {
			return log.Errore(err)
		}

		tx, err := db.Begin()
		stmt, err := tx.Prepare(`delete from database_instance_pool where pool = ?`)
		_, err = stmt.Exec(pool)
		if err != nil {
			tx.Rollback()
			return log.Errore(err)
		}
		stmt, err = tx.Prepare(`insert into database_instance_pool (hostname, port, pool, registered_at) values (?, ?, ?, now())`)
		for _, instanceKey := range instanceKeys {
			_, err := stmt.Exec(instanceKey.Hostname, instanceKey.Port, pool)
			if err != nil {
				tx.Rollback()
				return log.Errore(err)
			}
		}
		if err != nil {
			tx.Rollback()
			return log.Errore(err)
		}
		tx.Commit()

		return nil
	}
	return ExecDBWriteFunc(writeFunc)
}

func ReadClusterPoolInstances(clusterName string, pool string) (result [](*ClusterPoolInstance), err error) {
	args := sqlutils.Args()
	whereClause := ``
	if clusterName != "" {
		whereClause = `
			where
				database_instance.cluster_name = ?
				and ? in ('', pool)
		`
		args = append(args, clusterName, pool)
	}
	query := fmt.Sprintf(`
		select
			cluster_name,
			ifnull(alias, cluster_name) as alias,
			database_instance_pool.*
		from
			database_instance
			join database_instance_pool using (hostname, port)
			left join cluster_alias using (cluster_name)
		%s
		`, whereClause)
	err = db.QueryBakufu(query, args, func(m sqlutils.RowMap) error {
		clusterPoolInstance := ClusterPoolInstance{
			ClusterName:  m.GetString("cluster_name"),
			ClusterAlias: m.GetString("alias"),
			Pool:         m.GetString("pool"),
			Hostname:     m.GetString("hostname"),
			Port:         m.GetInt("port"),
		}
		result = append(result, &clusterPoolInstance)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

func ReadAllClusterPoolInstances() ([](*ClusterPoolInstance), error) {
	return ReadClusterPoolInstances("", "")
}

func ReadClusterPoolInstancesMap(clusterName string, pool string) (*PoolInstancesMap, error) {
	var poolInstancesMap = make(PoolInstancesMap)

	clusterPoolInstances, err := ReadClusterPoolInstances(clusterName, pool)
	if err != nil {
		return nil, nil
	}
	for _, clusterPoolInstance := range clusterPoolInstances {
		if _, ok := poolInstancesMap[clusterPoolInstance.Pool]; !ok {
			poolInstancesMap[clusterPoolInstance.Pool] = [](*InstanceKey){}
		}
		poolInstancesMap[clusterPoolInstance.Pool] = append(poolInstancesMap[clusterPoolInstance.Pool], &InstanceKey{Hostname: clusterPoolInstance.Hostname, Port: clusterPoolInstance.Port})
	}

	return &poolInstancesMap, nil
}
