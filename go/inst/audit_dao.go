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
	"github.com/rcrowley/go-metrics"
	"log/syslog"
	"os"
	"time"
)

var syslogWriter *syslog.Writer

var auditOperationCounter = metrics.NewCounter()

func init() {
	metrics.Register("audit.write", auditOperationCounter)
}

func EnableAuditSyslog() (err error) {
	syslogWriter, err = syslog.New(syslog.LOG_ERR, "bakufu")
	if err != nil {
		syslogWriter = nil
	}
	return err
}

func AuditOperation(auditType string, instanceKey *InstanceKey, message string) error {

	if instanceKey == nil {
		instanceKey = &InstanceKey{}
	}
	clusterName := ""
	if instanceKey.Hostname != "" {
		clusterName, _ = GetClusterName(instanceKey)
	}

	if config.Config.AuditLogFile != "" {
		go func() error {
			f, err := os.OpenFile(config.Config.AuditLogFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
			if err != nil {
				return log.Errore(err)
			}

			defer f.Close()
			text := fmt.Sprintf("%s\t%s\t%s\t%d\t[%s]\t%s\t\n", time.Now().Format(log.TimeFormat), auditType, instanceKey.Hostname, instanceKey.Port, clusterName, message)
			if _, err = f.WriteString(text); err != nil {
				return log.Errore(err)
			}
			return nil
		}()
	}
	_, err := db.ExecBakufu(`
			insert 
				into audit (
					audit_timestamp, audit_type, hostname, port, cluster_name, message
				) VALUES (
					NOW(), ?, ?, ?, ?, ?
				)
			`,
		auditType,
		instanceKey.Hostname,
		instanceKey.Port,
		clusterName,
		message,
	)
	if err != nil {
		return log.Errore(err)
	}
	logMessage := fmt.Sprintf("auditType:%s instance:%s cluster:%s message:%s", auditType, instanceKey.DisplayString(), clusterName, message)
	if syslogWriter != nil {
		go func() {
			syslogWriter.Info(logMessage)
		}()
	}
	log.Debugf(logMessage)
	auditOperationCounter.Inc(1)

	return err
}

func ReadRecentAudit(instanceKey *InstanceKey, page int) ([]Audit, error) {
	res := []Audit{}
	args := sqlutils.Args()
	whereCondition := ``
	if instanceKey != nil {
		whereCondition = `where hostname=? and port=?`
		args = append(args, instanceKey.Hostname, instanceKey.Port)
	}
	query := fmt.Sprintf(`
		select 
			audit_id,
			audit_timestamp,
			audit_type,
			hostname,
			port,
			message
		from 
			audit
		%s
		order by
			audit_timestamp desc
		limit ?
		offset ?
		`, whereCondition)
	args = append(args, config.Config.AuditPageSize, page*config.Config.AuditPageSize)
	err := db.QueryBakufu(query, args, func(m sqlutils.RowMap) error {
		audit := Audit{}
		audit.AuditId = m.GetInt64("audit_id")
		audit.AuditTimestamp = m.GetString("audit_timestamp")
		audit.AuditType = m.GetString("audit_type")
		audit.AuditInstanceKey.Hostname = m.GetString("hostname")
		audit.AuditInstanceKey.Port = m.GetInt("port")
		audit.Message = m.GetString("message")

		res = append(res, audit)
		return nil
	})

	if err != nil {
		log.Errore(err)
	}
	return res, err

}

func ExpireAudit() error {
	writeFunc := func() error {
		_, err := db.ExecBakufu(`
 		delete from
				audit
			where
				audit_timestamp < NOW() - INTERVAL ? DAY 
			`,
			config.Config.AuditPurgeDays,
		)
		return err
	}
	return ExecDBWriteFunc(writeFunc)
}
