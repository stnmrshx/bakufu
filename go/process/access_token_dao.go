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

func GenerateAccessToken(owner string) (publicToken string, err error) {
	publicToken = NewToken().Hash
	secretToken := NewToken().Hash

	_, err = db.ExecBakufu(`
			insert into access_token (
					public_token, secret_token, generated_at, generated_by, is_acquired, is_reentrant
				) values (
					?, ?, now(), ?, 0, 0
				)
			`,
		publicToken, secretToken, owner,
	)
	if err != nil {
		return publicToken, log.Errore(err)
	}
	return publicToken, nil
}

func AcquireAccessToken(publicToken string) (secretToken string, err error) {
	secretToken = ""
	sqlResult, err := db.ExecBakufu(`
			update access_token
				set
					is_acquired=1,
					acquired_at=now()
				where
					public_token=?
					and (
						(
							is_acquired=0
							and generated_at > now() - interval ? second
						)
						or is_reentrant=1
					)
			`,
		publicToken, config.Config.AccessTokenUseExpirySeconds,
	)
	if err != nil {
		return secretToken, log.Errore(err)
	}
	rows, err := sqlResult.RowsAffected()
	if err != nil {
		return secretToken, log.Errore(err)
	}
	if rows == 0 {
		return secretToken, log.Errorf("Cannot acquire token %s", publicToken)
	}
	query := `
		select secret_token from access_token where public_token=?
		`
	err = db.QueryBakufu(query, sqlutils.Args(publicToken), func(m sqlutils.RowMap) error {
		secretToken = m.GetString("secret_token")
		return nil
	})
	return secretToken, log.Errore(err)
}

func TokenIsValid(publicToken string, secretToken string) (result bool, err error) {
	query := `
		select
				count(*) as valid_token
			from
				access_token
		  where
				public_token=?
				and secret_token=?
				and (
					generated_at >= now() - interval ? minute
					or is_reentrant = 1
				)
		`
	err = db.QueryBakufu(query, sqlutils.Args(publicToken, secretToken, config.Config.AccessTokenExpiryMinutes), func(m sqlutils.RowMap) error {
		result = m.GetInt("valid_token") > 0
		return nil
	})
	return result, log.Errore(err)
}

func ExpireAccessTokens() error {
	_, err := db.ExecBakufu(`
			delete
				from access_token
			where
				generated_at < now() - interval ? minute
				and is_reentrant = 0
			`,
		config.Config.AccessTokenExpiryMinutes,
	)
	return log.Errore(err)
}
