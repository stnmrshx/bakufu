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
	"github.com/stnmrshx/bakufu/go/config"
	"github.com/stnmrshx/bakufu/go/db"
)

func BeginDowntime(instanceKey *InstanceKey, owner string, reason string, durationSeconds uint) error {
	if durationSeconds == 0 {
		durationSeconds = config.Config.MaintenanceExpireMinutes * 60
	}
	_, err := db.ExecBakufu(`
			insert 
				into database_instance_downtime (
					hostname, port, downtime_active, begin_timestamp, end_timestamp, owner, reason
				) VALUES (
					?, ?, 1, NOW(), NOW() + INTERVAL ? SECOND, ?, ?
				)
				on duplicate key update
					downtime_active=values(downtime_active),
					begin_timestamp=values(begin_timestamp),
					end_timestamp=values(end_timestamp),
					owner=values(owner),
					reason=values(reason)
			`,
		instanceKey.Hostname,
		instanceKey.Port,
		durationSeconds,
		owner,
		reason,
	)
	if err != nil {
		return log.Errore(err)
	}

	AuditOperation("begin-downtime", instanceKey, fmt.Sprintf("owner: %s, reason: %s", owner, reason))
	return nil
}

func EndDowntime(instanceKey *InstanceKey) error {
	res, err := db.ExecBakufu(`
			update
				database_instance_downtime
			set  
				downtime_active = NULL,
				end_timestamp = NOW()
			where
				hostname = ? 
				and port = ?
				and downtime_active = 1
			`,
		instanceKey.Hostname,
		instanceKey.Port,
	)
	if err != nil {
		return log.Errore(err)
	}

	if affected, _ := res.RowsAffected(); affected == 0 {
		err = fmt.Errorf("Instance is not in downtime mode: %+v", instanceKey)
	} else {
		AuditOperation("end-downtime", instanceKey, "")
	}
	return err
}

func ExpireDowntime() error {
	{
		res, err := db.ExecBakufu(`
			delete from
				database_instance_downtime
			where
				downtime_active is null
				and end_timestamp < NOW() - INTERVAL ? DAY 
			`,
			config.Config.MaintenancePurgeDays,
		)
		if err != nil {
			return log.Errore(err)
		}
		if rowsAffected, _ := res.RowsAffected(); rowsAffected > 0 {
			AuditOperation("expire-downtime", nil, fmt.Sprintf("Purged %d historical entries", rowsAffected))
		}
	}
	{
		res, err := db.ExecBakufu(`
			update
				database_instance_downtime
			set  
				downtime_active = NULL				
			where
				downtime_active = 1
				and end_timestamp < NOW() 
			`,
		)
		if err != nil {
			return log.Errore(err)
		}
		if rowsAffected, _ := res.RowsAffected(); rowsAffected > 0 {
			AuditOperation("expire-downtime", nil, fmt.Sprintf("Expired %d entries", rowsAffected))
		}
	}
	return nil
}
