/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package process

import (
	"database/sql"
	"fmt"
	"github.com/stnmrshx/golib/log"
	"github.com/stnmrshx/golib/sqlutils"
	"github.com/stnmrshx/bakufu/go/config"
	"github.com/stnmrshx/bakufu/go/db"
	"time"
)

const registrationPollSeconds = 10

type HealthStatus struct {
	Healthy        bool
	Hostname       string
	Token          string
	IsActiveNode   bool
	ActiveNode     string
	Error          error
	AvailableNodes []string
}

type BakufuExecutionMode string

const (
	BakufuExecutionCliMode  BakufuExecutionMode = "CLIMode"
	BakufuExecutionHttpMode                           = "HttpMode"
)

var continuousRegistrationInitiated bool = false

func RegisterNode(extraInfo string, command string, firstTime bool) (sql.Result, error) {
	if firstTime {
		db.ExecBakufu(`
			insert ignore into node_health_history
				(hostname, token, first_seen_active, extra_info, command)
			values
				(?, ?, NOW(), ?, ?)
			`,
			ThisHostname, ProcessToken.Hash, extraInfo, command,
		)
	}
	return db.ExecBakufu(`
			insert into node_health
				(hostname, token, last_seen_active, extra_info, command)
			values
				(?, ?, NOW(), ?, ?)
			on duplicate key update
				token=values(token),
				last_seen_active=values(last_seen_active),
				extra_info=if(values(extra_info) != '', values(extra_info), extra_info)
			`,
		ThisHostname, ProcessToken.Hash, extraInfo, command,
	)
}

func HealthTest() (*HealthStatus, error) {
	health := HealthStatus{Healthy: false, Hostname: ThisHostname, Token: ProcessToken.Hash}

	sqlResult, err := RegisterNode("", "", false)
	if err != nil {
		health.Error = err
		return &health, log.Errore(err)
	}
	rows, err := sqlResult.RowsAffected()
	if err != nil {
		health.Error = err
		return &health, log.Errore(err)
	}
	health.Healthy = (rows > 0)
	activeHostname, activeToken, isActive, err := ElectedNode()
	if err != nil {
		health.Error = err
		return &health, log.Errore(err)
	}
	health.ActiveNode = fmt.Sprintf("%s;%s", activeHostname, activeToken)
	health.IsActiveNode = isActive

	health.AvailableNodes, err = ReadAvailableNodes(true)

	return &health, nil
}

func ContinuousRegistration(extraInfo string, command string) {
	if continuousRegistrationInitiated {
		return
	}
	continuousRegistrationInitiated = true

	tickOperation := func(firstTime bool) {
		RegisterNode(extraInfo, command, firstTime)
		expireAvailableNodes()
	}
	tickOperation(true)
	go func() {
		registrationTick := time.Tick(time.Duration(registrationPollSeconds) * time.Second)
		for range registrationTick {
			go tickOperation(false)
		}
	}()
}

func expireAvailableNodes() error {
	_, err := db.ExecBakufu(`
			delete
				from node_health
			where
				last_seen_active < now() - interval ? second
			`,
		registrationPollSeconds*2,
	)
	return log.Errore(err)
}

func ExpireNodesHistory() error {
	_, err := db.ExecBakufu(`
			delete
				from node_health_history
			where
				first_seen_active < now() - interval ? hour
			`,
		config.Config.UnseenInstanceForgetHours,
	)
	return log.Errore(err)
}

func ReadAvailableNodes(onlyHttpNodes bool) ([]string, error) {
	res := []string{}
	extraInfo := ""
	if onlyHttpNodes {
		extraInfo = string(BakufuExecutionHttpMode)
	}
	query := `
		select
			concat(hostname, ';', token) as node
		from
			node_health
		where
			last_seen_active > now() - interval ? second
			and ? in (extra_info, '')
		order by
			hostname
		`

	err := db.QueryBakufu(query, sqlutils.Args(registrationPollSeconds*2, extraInfo), func(m sqlutils.RowMap) error {
		res = append(res, m.GetString("node"))
		return nil
	})
	if err != nil {
		log.Errore(err)
	}
	return res, err
}

func SimpleHealthTest() (*HealthStatus, error) {
	health := HealthStatus{Healthy: false, Hostname: ThisHostname, Token: ProcessToken.Hash}

	db, err := db.OpenBakufu()
	if err != nil {
		health.Error = err
		return &health, log.Errore(err)
	}

	if err = db.Ping(); err != nil {
		health.Error = err
		return &health, log.Errore(err)
	} else {
		health.Healthy = true
		return &health, nil
	}
}
