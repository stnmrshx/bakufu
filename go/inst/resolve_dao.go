/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package inst

import (
	"github.com/stnmrshx/golib/log"
	"github.com/stnmrshx/golib/sqlutils"
	"github.com/stnmrshx/bakufu/go/config"
	"github.com/stnmrshx/bakufu/go/db"
	"github.com/rcrowley/go-metrics"
)

var writeResolvedHostnameCounter = metrics.NewCounter()
var writeUnresolvedHostnameCounter = metrics.NewCounter()
var readResolvedHostnameCounter = metrics.NewCounter()
var readUnresolvedHostnameCounter = metrics.NewCounter()
var readAllResolvedHostnamesCounter = metrics.NewCounter()

func init() {
	metrics.Register("resolve.write_resolved", writeResolvedHostnameCounter)
	metrics.Register("resolve.write_unresolved", writeUnresolvedHostnameCounter)
	metrics.Register("resolve.read_resolved", readResolvedHostnameCounter)
	metrics.Register("resolve.read_unresolved", readUnresolvedHostnameCounter)
	metrics.Register("resolve.read_resolved_all", readAllResolvedHostnamesCounter)
}

func WriteResolvedHostname(hostname string, resolvedHostname string) error {
	writeFunc := func() error {
		_, err := db.ExecBakufu(`
			insert into  
					hostname_resolve (hostname, resolved_hostname, resolved_timestamp)
				values
					(?, ?, NOW())
				on duplicate key update
					resolved_hostname = VALUES(resolved_hostname), 
					resolved_timestamp = VALUES(resolved_timestamp)
			`,
			hostname,
			resolvedHostname)
		if err != nil {
			return log.Errore(err)
		}
		if hostname != resolvedHostname {
			_, err = db.ExecBakufu(`
			insert into  
					hostname_resolve_history (hostname, resolved_hostname, resolved_timestamp)
				values
					(?, ?, NOW())
				on duplicate key update 
					hostname=if(values(hostname) != resolved_hostname, values(hostname), hostname), 
					resolved_timestamp=values(resolved_timestamp)
			`,
				hostname,
				resolvedHostname)
		}
		log.Debugf("WriteResolvedHostname: resolved %s to %s", hostname, resolvedHostname)
		writeResolvedHostnameCounter.Inc(1)
		return nil
	}
	return ExecDBWriteFunc(writeFunc)
}

func ReadResolvedHostname(hostname string) (string, error) {
	var resolvedHostname string = ""

	query := `
		select 
			resolved_hostname
		from 
			hostname_resolve
		where
			hostname = ?
		`

	err := db.QueryBakufu(query, sqlutils.Args(hostname), func(m sqlutils.RowMap) error {
		resolvedHostname = m.GetString("resolved_hostname")
		return nil
	})
	readResolvedHostnameCounter.Inc(1)

	if err != nil {
		log.Errore(err)
	}
	return resolvedHostname, err
}

func readAllHostnameResolves() ([]HostnameResolve, error) {
	res := []HostnameResolve{}
	query := `
		select 
			hostname, 
			resolved_hostname  
		from 
			hostname_resolve
		`
	err := db.QueryBakufuRowsMap(query, func(m sqlutils.RowMap) error {
		hostnameResolve := HostnameResolve{hostname: m.GetString("hostname"), resolvedHostname: m.GetString("resolved_hostname")}

		res = append(res, hostnameResolve)
		return nil
	})
	readAllResolvedHostnamesCounter.Inc(1)

	if err != nil {
		log.Errore(err)
	}
	return res, err
}

func readUnresolvedHostname(hostname string) (string, error) {
	unresolvedHostname := hostname

	query := `
	   		select
	   			unresolved_hostname
	   		from
	   			hostname_unresolve
	   		where
	   			hostname = ?
	   		`

	err := db.QueryBakufu(query, sqlutils.Args(hostname), func(m sqlutils.RowMap) error {
		unresolvedHostname = m.GetString("unresolved_hostname")
		return nil
	})
	readUnresolvedHostnameCounter.Inc(1)

	if err != nil {
		log.Errore(err)
	}
	return unresolvedHostname, err
}

func readMissingKeysToResolve() (result InstanceKeyMap, err error) {
	query := `
   		select 
   				hostname_unresolve.unresolved_hostname,
   				database_instance.port
   			from 
   				database_instance 
   				join hostname_unresolve on (database_instance.hostname = hostname_unresolve.hostname) 
   				left join hostname_resolve on (database_instance.hostname = hostname_resolve.resolved_hostname) 
   			where 
   				hostname_resolve.hostname is null
	   		`

	err = db.QueryBakufuRowsMap(query, func(m sqlutils.RowMap) error {
		instanceKey := InstanceKey{Hostname: m.GetString("unresolved_hostname"), Port: m.GetInt("port")}
		result.AddKey(instanceKey)
		return nil
	})

	if err != nil {
		log.Errore(err)
	}
	return result, err
}

func WriteHostnameUnresolve(instanceKey *InstanceKey, unresolvedHostname string) error {
	writeFunc := func() error {
		_, err := db.ExecBakufu(`
        	insert into hostname_unresolve (
        		hostname,
        		unresolved_hostname,
        		last_registered)
        	values (?, ?, NOW())
        	on duplicate key update
        		unresolved_hostname=values(unresolved_hostname),
        		last_registered=now()
				`, instanceKey.Hostname, unresolvedHostname,
		)
		if err != nil {
			return log.Errore(err)
		}
		_, err = db.ExecBakufu(`
	        	replace into hostname_unresolve_history (
        		hostname,
        		unresolved_hostname,
        		last_registered)
        	values (?, ?, NOW())
				`, instanceKey.Hostname, unresolvedHostname,
		)
		writeUnresolvedHostnameCounter.Inc(1)
		return nil
	}
	return ExecDBWriteFunc(writeFunc)
}

func DeregisterHostnameUnresolve(instanceKey *InstanceKey) error {
	writeFunc := func() error {
		_, err := db.ExecBakufu(`
        	delete from hostname_unresolve 
				where hostname=?
				`, instanceKey.Hostname,
		)
		return log.Errore(err)
	}
	return ExecDBWriteFunc(writeFunc)
}

func ExpireHostnameUnresolve() error {
	writeFunc := func() error {
		_, err := db.ExecBakufu(`
        	delete from hostname_unresolve 
				where last_registered < NOW() - INTERVAL ? MINUTE
				`, config.Config.ExpiryHostnameResolvesMinutes,
		)
		return log.Errore(err)
	}
	return ExecDBWriteFunc(writeFunc)
}

func ForgetExpiredHostnameResolves() error {
	_, err := db.ExecBakufu(`
			delete 
				from hostname_resolve 
			where 
				resolved_timestamp < NOW() - interval (? * 2) minute`,
		config.Config.ExpiryHostnameResolvesMinutes,
	)
	return err
}

func DeleteInvalidHostnameResolves() error {
	var invalidHostnames []string

	query := `
		select 
		    early.hostname
		  from 
		    hostname_resolve as latest 
		    join hostname_resolve early on (latest.resolved_hostname = early.hostname and latest.hostname = early.resolved_hostname) 
		  where 
		    latest.hostname != latest.resolved_hostname 
		    and latest.resolved_timestamp > early.resolved_timestamp
	   	`

	err := db.QueryBakufuRowsMap(query, func(m sqlutils.RowMap) error {
		invalidHostnames = append(invalidHostnames, m.GetString("hostname"))
		return nil
	})
	if err != nil {
		return err
	}

	for _, invalidHostname := range invalidHostnames {
		_, err = db.ExecBakufu(`
			delete 
				from hostname_resolve 
			where 
				hostname = ?`,
			invalidHostname,
		)
		log.Errore(err)
	}
	return err
}

func deleteHostnameResolves() error {
	_, err := db.ExecBakufu(`
			delete 
				from hostname_resolve`,
	)
	return err
}
