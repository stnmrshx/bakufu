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
	"github.com/stnmrshx/bakufu/go/config"
	"github.com/stnmrshx/bakufu/go/db"
)

func ReadActiveMaintenance() ([]Maintenance, error) {
	res := []Maintenance{}
	query := `
		select 
			database_instance_maintenance_id,
			hostname,
			port,
			begin_timestamp,
			timestampdiff(second, begin_timestamp, now()) as seconds_elapsed,
			maintenance_active,
			owner,
			reason
		from 
			database_instance_maintenance
		where
			maintenance_active = 1
		order by
			database_instance_maintenance_id
		`
	err := db.QueryBakufuRowsMap(query, func(m sqlutils.RowMap) error {
		maintenance := Maintenance{}
		maintenance.MaintenanceId = m.GetUint("database_instance_maintenance_id")
		maintenance.Key.Hostname = m.GetString("hostname")
		maintenance.Key.Port = m.GetInt("port")
		maintenance.BeginTimestamp = m.GetString("begin_timestamp")
		maintenance.SecondsElapsed = m.GetUint("seconds_elapsed")
		maintenance.IsActive = m.GetBool("maintenance_active")
		maintenance.Owner = m.GetString("owner")
		maintenance.Reason = m.GetString("reason")

		res = append(res, maintenance)
		return nil
	})

	if err != nil {
		log.Errore(err)
	}
	return res, err

}

func BeginBoundedMaintenance(instanceKey *InstanceKey, owner string, reason string, durationSeconds uint) (int64, error) {
	var maintenanceToken int64 = 0
	if durationSeconds == 0 {
		durationSeconds = config.Config.MaintenanceExpireMinutes * 60
	}
	res, err := db.ExecBakufu(`
			insert ignore
				into database_instance_maintenance (
					hostname, port, maintenance_active, begin_timestamp, end_timestamp, owner, reason
				) VALUES (
					?, ?, 1, NOW(), NOW() + INTERVAL ? SECOND, ?, ?
				)
			`,
		instanceKey.Hostname,
		instanceKey.Port,
		durationSeconds,
		owner,
		reason,
	)
	if err != nil {
		return maintenanceToken, log.Errore(err)
	}

	if affected, _ := res.RowsAffected(); affected == 0 {
		err = fmt.Errorf("Cannot begin maintenance for instance: %+v; maintenance reason: %+v", instanceKey, reason)
	} else {
		maintenanceToken, _ = res.LastInsertId()
		AuditOperation("begin-maintenance", instanceKey, fmt.Sprintf("maintenanceToken: %d, owner: %s, reason: %s", maintenanceToken, owner, reason))
	}
	return maintenanceToken, err
}

func BeginMaintenance(instanceKey *InstanceKey, owner string, reason string) (int64, error) {
	return BeginBoundedMaintenance(instanceKey, owner, reason, 0)
}

func EndMaintenanceByInstanceKey(instanceKey *InstanceKey) error {
	res, err := db.ExecBakufu(`
			update
				database_instance_maintenance
			set  
				maintenance_active = NULL,
				end_timestamp = NOW()
			where
				hostname = ? 
				and port = ?
				and maintenance_active = 1
			`,
		instanceKey.Hostname,
		instanceKey.Port,
	)
	if err != nil {
		return log.Errore(err)
	}

	if affected, _ := res.RowsAffected(); affected == 0 {
		err = fmt.Errorf("Instance is not in maintenance mode: %+v", instanceKey)
	} else {
		AuditOperation("end-maintenance", instanceKey, "")
	}
	return err
}

func ReadMaintenanceInstanceKey(maintenanceToken int64) (*InstanceKey, error) {
	var res *InstanceKey
	query := `
		select 
			hostname, port 
		from 
			database_instance_maintenance 
		where
			database_instance_maintenance_id = ?
			`

	err := db.QueryBakufu(query, sqlutils.Args(maintenanceToken), func(m sqlutils.RowMap) error {
		instanceKey, merr := NewInstanceKeyFromStrings(m.GetString("hostname"), m.GetString("port"))
		if merr != nil {
			return merr
		}

		res = instanceKey
		return nil
	})

	if err != nil {
		log.Errore(err)
	}
	return res, err
}

func EndMaintenance(maintenanceToken int64) error {
	res, err := db.ExecBakufu(`
			update
				database_instance_maintenance
			set  
				maintenance_active = NULL,
				end_timestamp = NOW()
			where
				database_instance_maintenance_id = ? 
			`,
		maintenanceToken,
	)
	if err != nil {
		return log.Errore(err)
	}
	if affected, _ := res.RowsAffected(); affected == 0 {
		err = fmt.Errorf("Instance is not in maintenance mode; token = %+v", maintenanceToken)
	} else {
		instanceKey, _ := ReadMaintenanceInstanceKey(maintenanceToken)
		AuditOperation("end-maintenance", instanceKey, fmt.Sprintf("maintenanceToken: %d", maintenanceToken))
	}
	return err
}

func ExpireMaintenance() error {
	{
		res, err := db.ExecBakufu(`
			delete from
				database_instance_maintenance
			where
				maintenance_active is null
				and end_timestamp < NOW() - INTERVAL ? DAY 
			`,
			config.Config.MaintenancePurgeDays,
		)
		if err != nil {
			return log.Errore(err)
		}
		if rowsAffected, _ := res.RowsAffected(); rowsAffected > 0 {
			AuditOperation("expire-maintenance", nil, fmt.Sprintf("Purged historical entries: %d", rowsAffected))
		}
	}
	{
		res, err := db.ExecBakufu(`
			update
				database_instance_maintenance
			set  
				maintenance_active = NULL				
			where
				maintenance_active = 1
				and end_timestamp < NOW() 
			`,
		)
		if err != nil {
			return log.Errore(err)
		}
		if rowsAffected, _ := res.RowsAffected(); rowsAffected > 0 {
			AuditOperation("expire-maintenance", nil, fmt.Sprintf("Expired bounded: %d", rowsAffected))
		}
	}

	return nil
}
