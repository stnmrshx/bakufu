/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package logic

import (
	"fmt"
	"github.com/stnmrshx/golib/log"
	"github.com/stnmrshx/golib/sqlutils"
	"github.com/stnmrshx/bakufu/go/config"
	"github.com/stnmrshx/bakufu/go/db"
	"github.com/stnmrshx/bakufu/go/inst"
)

func WriteAsyncRequest(asyncRequest *AsyncRequest) error {
	if asyncRequest.OperatedInstanceKey == nil {
		return log.Errorf("WriteAsyncRequest received asyncRequest.OperatedInstanceKey for command %+v", asyncRequest.Command)
	}
	destinationKey := asyncRequest.DestinationKey
	if destinationKey == nil {
		destinationKey = &inst.InstanceKey{}
	}
	writeFunc := func() error {
		_, err := db.ExecBakufu(`
			insert into async_request (
					command, hostname, port, destination_hostname, destination_port, pattern, gtid_hint, story, begin_timestamp, end_timestamp
				) values (
					?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL
				)
				`, asyncRequest.Command, asyncRequest.OperatedInstanceKey.Hostname, asyncRequest.OperatedInstanceKey.Port,
			destinationKey.Hostname, destinationKey.Port, asyncRequest.Pattern, string(asyncRequest.GTIDHint), asyncRequest.Story,
		)
		return log.Errore(err)
	}
	return inst.ExecDBWriteFunc(writeFunc)
}

func ReadPendingAsyncRequests(limit int) (res [](*AsyncRequest), err error) {
	limitClause := ``
	args := sqlutils.Args()
	if limit > 0 {
		limitClause = `limit ?`
		args = append(args, limit)
	}
	query := fmt.Sprintf(`
		select
			request_id,
			command,
			hostname,
			port,
			destination_hostname,
			destination_port,
			pattern,
			gtid_hint,
			story
		from
			async_request
		where
			begin_timestamp IS NULL
		order by
			request_id asc
		%s
		`, limitClause)
	err = db.QueryBakufu(query, args, func(m sqlutils.RowMap) error {
		asyncRequest := NewEmptyAsyncRequest()
		asyncRequest.Id = m.GetInt64("request_id")
		asyncRequest.Command = m.GetString("command")

		asyncRequest.OperatedInstanceKey = &inst.InstanceKey{}
		asyncRequest.OperatedInstanceKey.Hostname = m.GetString("hostname")
		asyncRequest.OperatedInstanceKey.Port = m.GetInt("port")

		if m.GetString("destination_hostname") != "" {
			asyncRequest.DestinationKey = &inst.InstanceKey{}
			asyncRequest.DestinationKey.Hostname = m.GetString("destination_hostname")
			asyncRequest.DestinationKey.Port = m.GetInt("destination_port")
		}

		asyncRequest.Pattern = m.GetString("pattern")
		asyncRequest.GTIDHint = inst.OperationGTIDHint(m.GetString("gtid_hint"))
		asyncRequest.Story = m.GetString("story")

		res = append(res, asyncRequest)
		return nil
	})

	if err != nil {
		log.Errore(err)
	}
	return res, err
}

func BeginAsyncRequest(asyncRequest *AsyncRequest) (bool, error) {
	sqlResult, err := db.ExecBakufu(`
			update
				async_request
			set
				begin_timestamp = NOW()
			where
				request_id = ?
				and begin_timestamp IS NULL
			`, asyncRequest.Id,
	)
	if err != nil {
		return false, log.Errore(err)
	}
	rows, err := sqlResult.RowsAffected()
	return (rows > 0), err

}

func ExpireAsyncRequests() error {
	_, err := db.ExecBakufu(`
			update
				async_request
			set
				end_timestamp = NOW()
			where
				end_timestamp IS NULL
				and begin_timestamp < NOW() - INTERVAL ? MINUTE
			`, config.Config.MaintenanceExpireMinutes,
	)
	if err != nil {
		log.Errore(err)
	}
	_, err = db.ExecBakufu(`
			delete from
				async_request
			where
				end_timestamp IS NOT NULL
				and begin_timestamp < NOW() - INTERVAL ? DAY
			`, config.Config.MaintenancePurgeDays,
	)
	return log.Errore(err)
}
