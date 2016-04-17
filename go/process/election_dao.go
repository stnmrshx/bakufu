/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package process

import (
	"github.com/stnmrshx/golib/log"
	"github.com/stnmrshx/golib/sqlutils"
	"github.com/stnmrshx/bakufu/go/config"
	"github.com/stnmrshx/bakufu/go/db"
)

func AttemptElection() (bool, error) {
	sqlResult, err := db.ExecBakufu(`
			insert ignore into active_node (
					anchor, hostname, token, last_seen_active
				) values (
					1, ?, ?, now()
				) on duplicate key update
					hostname = if(last_seen_active < now() - interval ? second, values(hostname), hostname),
					token    = if(last_seen_active < now() - interval ? second, values(token),    token),
					last_seen_active = if(hostname = values(hostname) and token = values(token), values(last_seen_active), last_seen_active)					
			`,
		ThisHostname, ProcessToken.Hash, config.Config.ActiveNodeExpireSeconds, config.Config.ActiveNodeExpireSeconds,
	)
	if err != nil {
		return false, log.Errore(err)
	}
	rows, err := sqlResult.RowsAffected()
	return (rows > 0), log.Errore(err)
}

func GrabElection() error {
	_, err := db.ExecBakufu(`
			replace into active_node (
					anchor, hostname, token, last_seen_active
				) values (
					1, ?, ?, NOW()
				)
			`,
		ThisHostname, ProcessToken.Hash,
	)
	return log.Errore(err)
}

func Reelect() error {
	_, err := db.ExecBakufu(`delete from active_node where anchor = 1`)
	return log.Errore(err)
}

func ElectedNode() (hostname string, token string, isElected bool, err error) {
	query := `
		select 
			hostname,
			token
		from 
			active_node
		where
			anchor = 1
		`
	err = db.QueryBakufuRowsMap(query, func(m sqlutils.RowMap) error {
		hostname = m.GetString("hostname")
		token = m.GetString("token")
		return nil
	})

	if err != nil {
		log.Errore(err)
	}
	isElected = (hostname == ThisHostname && token == ProcessToken.Hash)
	return hostname, token, isElected, err
}
