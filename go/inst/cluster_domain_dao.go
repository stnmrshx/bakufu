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

func ReadClusterDomainName(clusterName string) (string, error) {
	domainName := ""
	query := `
		select 
			domain_name
		from 
			cluster_domain_name
		where
			cluster_name = ?
		`
	err := db.QueryBakufu(query, sqlutils.Args(clusterName), func(m sqlutils.RowMap) error {
		domainName = m.GetString("domain_name")
		return nil
	})
	if err != nil {
		return "", err
	}
	if domainName == "" {
		err = fmt.Errorf("No domain name found for cluster %s", clusterName)
	}
	return domainName, err

}

func WriteClusterDomainName(clusterName string, domainName string) error {
	writeFunc := func() error {
		_, err := db.ExecBakufu(`
			insert into  
					cluster_domain_name (cluster_name, domain_name, last_registered)
				values
					(?, ?, NOW())
				on duplicate key update
					domain_name=values(domain_name),
					last_registered=values(last_registered)
			`,
			clusterName,
			domainName)
		if err != nil {
			return log.Errore(err)
		}

		return nil
	}
	return ExecDBWriteFunc(writeFunc)
}

func ExpireClusterDomainName() error {
	writeFunc := func() error {
		_, err := db.ExecBakufu(`
        	delete from cluster_domain_name 
				where last_registered < NOW() - INTERVAL ? MINUTE
				`, config.Config.ExpiryHostnameResolvesMinutes,
		)
		return log.Errore(err)
	}
	return ExecDBWriteFunc(writeFunc)
}
