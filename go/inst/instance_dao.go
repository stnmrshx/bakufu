/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package inst

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/stnmrshx/golib/log"
	"github.com/stnmrshx/golib/math"
	"github.com/stnmrshx/golib/sqlutils"
	"github.com/stnmrshx/bakufu/go/config"
	"github.com/stnmrshx/bakufu/go/db"
	"github.com/pmylund/go-cache"
	"github.com/rcrowley/go-metrics"
)

const backendDBConcurrency = 20

var instanceReadChan = make(chan bool, backendDBConcurrency)
var instanceWriteChan = make(chan bool, backendDBConcurrency)

type InstancesByCountSlaveHosts [](*Instance)

func (this InstancesByCountSlaveHosts) Len() int      { return len(this) }
func (this InstancesByCountSlaveHosts) Swap(i, j int) { this[i], this[j] = this[j], this[i] }
func (this InstancesByCountSlaveHosts) Less(i, j int) bool {
	return len(this[i].SlaveHosts) < len(this[j].SlaveHosts)
}

var instanceKeyInformativeClusterName = cache.New(time.Duration(config.Config.DiscoveryPollSeconds/2)*time.Second, time.Second)

var readTopologyInstanceCounter = metrics.NewCounter()
var readInstanceCounter = metrics.NewCounter()
var writeInstanceCounter = metrics.NewCounter()

func init() {
	metrics.Register("instance.read_topology", readTopologyInstanceCounter)
	metrics.Register("instance.read", readInstanceCounter)
	metrics.Register("instance.write", writeInstanceCounter)
}

func ExecDBWriteFunc(f func() error) error {
	instanceWriteChan <- true
	defer func() { recover(); <-instanceWriteChan }()
	res := f()
	return res
}

func logReadTopologyInstanceError(instanceKey *InstanceKey, hint string, err error) error {
	if err == nil {
		return nil
	}
	return log.Errorf("ReadTopologyInstance(%+v) %+v: %+v", *instanceKey, hint, err)
}

func ReadTopologyInstance(instanceKey *InstanceKey) (*Instance, error) {
	defer func() {
		if err := recover(); err != nil {
			logReadTopologyInstanceError(instanceKey, "Unexpected, aborting", fmt.Errorf("%+v", err))
		}
	}()

	instance := NewInstance()
	instanceFound := false
	foundByShowSlaveHosts := false
	longRunningProcesses := []Process{}
	resolvedHostname := ""
	maxScaleMasterHostname := ""
	isMaxScale := false
	isMaxScale110 := false
	slaveStatusFound := false
	var resolveErr error

	_ = UpdateInstanceLastAttemptedCheck(instanceKey)

	if !instanceKey.IsValid() {
		return instance, fmt.Errorf("ReadTopologyInstance will not act on invalid instance key: %+v", *instanceKey)
	}

	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		goto Cleanup
	}

	instance.Key = *instanceKey

	{
		err = sqlutils.QueryRowsMap(db, "show variables like 'maxscale%'", func(m sqlutils.RowMap) error {
			variableName := m.GetString("Variable_name")
			if variableName == "MAXSCALE_VERSION" {
				originalVersion := m.GetString("Value")
				if originalVersion == "" {
					originalVersion = m.GetString("value")
				}
				if originalVersion == "" {
					originalVersion = "0.0.0"
				}
				instance.Version = originalVersion + "-maxscale"
				instance.ServerID = 0
				instance.ServerUUID = ""
				instance.Uptime = 0
				instance.Binlog_format = "INHERIT"
				instance.ReadOnly = true
				instance.LogBinEnabled = true
				instance.LogSlaveUpdatesEnabled = true
				resolvedHostname = instance.Key.Hostname
				UpdateResolvedHostname(resolvedHostname, resolvedHostname)
				isMaxScale = true
			}
			return nil
		})
		if err != nil {
			logReadTopologyInstanceError(instanceKey, "show variables like 'maxscale%'", err)
		}
	}

	if isMaxScale && strings.Contains(instance.Version, "1.1.0") {
		isMaxScale110 = true
	}
	if isMaxScale110 {
		err = db.QueryRow("select @@hostname").Scan(&maxScaleMasterHostname)
		if err != nil {
			goto Cleanup
		}
	}
	if isMaxScale {
		if isMaxScale110 {
			db.QueryRow("select @@server_id").Scan(&instance.ServerID)
		} else {
			db.QueryRow("select @@global.server_id").Scan(&instance.ServerID)
			db.QueryRow("select @@global.server_uuid").Scan(&instance.ServerUUID)
		}
	}

	if !isMaxScale {
		var mysqlHostname, mysqlReportHost string
		err = db.QueryRow("select @@global.hostname, ifnull(@@global.report_host, ''), @@global.server_id, @@global.version, @@global.read_only, @@global.binlog_format, @@global.log_bin, @@global.log_slave_updates").Scan(
			&mysqlHostname, &mysqlReportHost, &instance.ServerID, &instance.Version, &instance.ReadOnly, &instance.Binlog_format, &instance.LogBinEnabled, &instance.LogSlaveUpdatesEnabled)
		if err != nil {
			goto Cleanup
		}
		switch strings.ToLower(config.Config.MySQLHostnameResolveMethod) {
		case "none":
			resolvedHostname = instance.Key.Hostname
		case "default", "hostname", "@@hostname":
			resolvedHostname = mysqlHostname
		case "report_host", "@@report_host":
			if mysqlReportHost == "" {
				err = fmt.Errorf("MySQLHostnameResolveMethod configured to use @@report_host but %+v has NULL/empty @@report_host", instanceKey)
				goto Cleanup
			}
			resolvedHostname = mysqlReportHost
		default:
			resolvedHostname = instance.Key.Hostname
		}

		if instance.IsOracleMySQL() && !instance.IsSmallerMajorVersionByString("5.6") {
			var masterInfoRepositoryOnTable bool
			_ = db.QueryRow("select @@global.gtid_mode = 'ON', @@global.server_uuid, @@global.gtid_purged, @@global.master_info_repository = 'TABLE'").Scan(&instance.SupportsOracleGTID, &instance.ServerUUID, &instance.GtidPurged, &masterInfoRepositoryOnTable)
			if masterInfoRepositoryOnTable {
				_ = db.QueryRow("select count(*) > 0 and MAX(User_name) != '' from mysql.slave_master_info").Scan(&instance.ReplicationCredentialsAvailable)
			}
		}
	}
	{
		var dummy string
		err = db.QueryRow("show global status like 'Uptime'").Scan(&dummy, &instance.Uptime)

		if err != nil {
			logReadTopologyInstanceError(instanceKey, "show global status like 'Uptime'", err)
		}
	}
	if resolvedHostname != instance.Key.Hostname {
		UpdateResolvedHostname(instance.Key.Hostname, resolvedHostname)
		instance.Key.Hostname = resolvedHostname
	}
	if instance.Key.Hostname == "" {
		err = fmt.Errorf("ReadTopologyInstance: empty hostname (%+v). Bailing out", *instanceKey)
		goto Cleanup
	}
	if config.Config.DataCenterPattern != "" {
		if pattern, err := regexp.Compile(config.Config.DataCenterPattern); err == nil {
			match := pattern.FindStringSubmatch(instance.Key.Hostname)
			if len(match) != 0 {
				instance.DataCenter = match[1]
			}
		}
	}
	if config.Config.PhysicalEnvironmentPattern != "" {
		if pattern, err := regexp.Compile(config.Config.PhysicalEnvironmentPattern); err == nil {
			match := pattern.FindStringSubmatch(instance.Key.Hostname)
			if len(match) != 0 {
				instance.PhysicalEnvironment = match[1]
			}
		}
	}

	err = sqlutils.QueryRowsMap(db, "show slave status", func(m sqlutils.RowMap) error {
		instance.HasReplicationCredentials = (m.GetString("Master_User") != "")
		instance.Slave_IO_Running = (m.GetString("Slave_IO_Running") == "Yes")
		if isMaxScale110 {
			instance.Slave_IO_Running = instance.Slave_IO_Running && (m.GetString("Slave_IO_State") == "Binlog Dump")
		}
		instance.Slave_SQL_Running = (m.GetString("Slave_SQL_Running") == "Yes")
		instance.ReadBinlogCoordinates.LogFile = m.GetString("Master_Log_File")
		instance.ReadBinlogCoordinates.LogPos = m.GetInt64("Read_Master_Log_Pos")
		instance.ExecBinlogCoordinates.LogFile = m.GetString("Relay_Master_Log_File")
		instance.ExecBinlogCoordinates.LogPos = m.GetInt64("Exec_Master_Log_Pos")
		instance.IsDetached, _, _ = instance.ExecBinlogCoordinates.DetachedCoordinates()
		instance.RelaylogCoordinates.LogFile = m.GetString("Relay_Log_File")
		instance.RelaylogCoordinates.LogPos = m.GetInt64("Relay_Log_Pos")
		instance.RelaylogCoordinates.Type = RelayLog
		instance.LastSQLError = m.GetString("Last_SQL_Error")
		instance.LastIOError = m.GetString("Last_IO_Error")
		instance.SQLDelay = m.GetUintD("SQL_Delay", 0)
		instance.UsingOracleGTID = (m.GetIntD("Auto_Position", 0) == 1)
		instance.ExecutedGtidSet = m.GetStringD("Executed_Gtid_Set", "")
		instance.UsingMariaDBGTID = (m.GetStringD("Using_Gtid", "No") != "No")
		instance.HasReplicationFilters = ((m.GetStringD("Replicate_Do_DB", "") != "") || (m.GetStringD("Replicate_Ignore_DB", "") != "") || (m.GetStringD("Replicate_Do_Table", "") != "") || (m.GetStringD("Replicate_Ignore_Table", "") != "") || (m.GetStringD("Replicate_Wild_Do_Table", "") != "") || (m.GetStringD("Replicate_Wild_Ignore_Table", "") != ""))

		masterHostname := m.GetString("Master_Host")
		if isMaxScale110 {
			masterHostname = maxScaleMasterHostname
		}
		masterKey, err := NewInstanceKeyFromStrings(masterHostname, m.GetString("Master_Port"))
		if err != nil {
			logReadTopologyInstanceError(instanceKey, "NewInstanceKeyFromStrings", err)
		}
		masterKey.Hostname, resolveErr = ResolveHostname(masterKey.Hostname)
		if resolveErr != nil {
			logReadTopologyInstanceError(instanceKey, fmt.Sprintf("ResolveHostname(%+v)", masterKey.Hostname), resolveErr)
		}
		instance.MasterKey = *masterKey
		instance.SecondsBehindMaster = m.GetNullInt64("Seconds_Behind_Master")
		instance.SlaveLagSeconds = instance.SecondsBehindMaster
		slaveStatusFound = true
		return nil
	})
	if err != nil {
		goto Cleanup
	}
	if isMaxScale && !slaveStatusFound {
		err = fmt.Errorf("No 'SHOW SLAVE STATUS' output found for a MaxScale instance: %+v", instanceKey)
		goto Cleanup
	}

	if instance.LogBinEnabled {
		err = sqlutils.QueryRowsMap(db, "show master status", func(m sqlutils.RowMap) error {
			var err error
			instance.SelfBinlogCoordinates.LogFile = m.GetString("File")
			instance.SelfBinlogCoordinates.LogPos = m.GetInt64("Position")
			return err
		})
		if err != nil {
			goto Cleanup
		}
	}

	instanceFound = true

	if config.Config.DiscoverByShowSlaveHosts || isMaxScale {
		err := sqlutils.QueryRowsMap(db, `show slave hosts`,
			func(m sqlutils.RowMap) error {
				slaveKey, err := NewInstanceKeyFromStrings(m.GetString("Host"), m.GetString("Port"))
				slaveKey.Hostname, resolveErr = ResolveHostname(slaveKey.Hostname)
				if err == nil {
					instance.AddSlaveKey(slaveKey)
					foundByShowSlaveHosts = true
				}
				return err
			})

		logReadTopologyInstanceError(instanceKey, "show slave hosts", err)
	}
	if !foundByShowSlaveHosts && !isMaxScale {
		err := sqlutils.QueryRowsMap(db, `
        	select
        		substring_index(host, ':', 1) as slave_hostname
        	from
        		information_schema.processlist
        	where
        		command='Binlog Dump'
        		or command='Binlog Dump GTID'
        		`,
			func(m sqlutils.RowMap) error {
				cname, resolveErr := ResolveHostname(m.GetString("slave_hostname"))
				if resolveErr != nil {
					logReadTopologyInstanceError(instanceKey, "ResolveHostname: processlist", resolveErr)
				}
				slaveKey := InstanceKey{Hostname: cname, Port: instance.Key.Port}
				instance.AddSlaveKey(&slaveKey)
				return err
			})

		logReadTopologyInstanceError(instanceKey, "processlist", err)
	}

	if config.Config.ReadLongRunningQueries && !isMaxScale {
		err := sqlutils.QueryRowsMap(db, `
				  select
				    id,
				    user,
				    host,
				    db,
				    command,
				    time,
				    state,
				    left(processlist.info, 1024) as info,
				    now() - interval time second as started_at
				  from
				    information_schema.processlist
				  where
				    time > 60
				    and command != 'Sleep'
				    and id != connection_id()
				    and user != 'system user'
				    and command != 'Binlog dump'
				    and command != 'Binlog Dump GTID'
				    and user != 'event_scheduler'
				  order by
				    time desc
        		`,
			func(m sqlutils.RowMap) error {
				process := Process{}
				process.Id = m.GetInt64("id")
				process.User = m.GetString("user")
				process.Host = m.GetString("host")
				process.Db = m.GetString("db")
				process.Command = m.GetString("command")
				process.Time = m.GetInt64("time")
				process.State = m.GetString("state")
				process.Info = m.GetString("info")
				process.StartedAt = m.GetString("started_at")

				longRunningProcesses = append(longRunningProcesses, process)
				return nil
			})

		logReadTopologyInstanceError(instanceKey, "processlist, long queries", err)
	}

	instance.UsingPseudoGTID = false
	if config.Config.DetectPseudoGTIDQuery != "" && !isMaxScale {
		if resultData, err := sqlutils.QueryResultData(db, config.Config.DetectPseudoGTIDQuery); err == nil {
			if len(resultData) > 0 {
				if len(resultData[0]) > 0 {
					if resultData[0][0].Valid && resultData[0][0].String == "1" {
						instance.UsingPseudoGTID = true
					}
				}
			}
		} else {
			logReadTopologyInstanceError(instanceKey, "DetectPseudoGTIDQuery", err)
		}
	}

	if config.Config.SlaveLagQuery != "" && !isMaxScale {
		err := db.QueryRow(config.Config.SlaveLagQuery).Scan(&instance.SlaveLagSeconds)
		if err != nil {
			instance.SlaveLagSeconds = instance.SecondsBehindMaster
			logReadTopologyInstanceError(instanceKey, "SlaveLagQuery", err)
		}
	}

	if config.Config.DetectDataCenterQuery != "" && !isMaxScale {
		err := db.QueryRow(config.Config.DetectDataCenterQuery).Scan(&instance.DataCenter)
		logReadTopologyInstanceError(instanceKey, "DetectDataCenterQuery", err)
	}

	if config.Config.DetectPhysicalEnvironmentQuery != "" && !isMaxScale {
		err := db.QueryRow(config.Config.DetectPhysicalEnvironmentQuery).Scan(&instance.PhysicalEnvironment)
		logReadTopologyInstanceError(instanceKey, "DetectPhysicalEnvironmentQuery", err)
	}

	{
		err = ReadInstanceClusterAttributes(instance)
		logReadTopologyInstanceError(instanceKey, "ReadInstanceClusterAttributes", err)
	}
	{
		err = ReadInstancePromotionRule(instance)
		logReadTopologyInstanceError(instanceKey, "ReadInstanceClusterAttributes", err)
	}
	if instance.ReplicationDepth == 0 && config.Config.DetectClusterAliasQuery != "" && !isMaxScale {
		clusterAlias := ""
		err := db.QueryRow(config.Config.DetectClusterAliasQuery).Scan(&clusterAlias)
		if err != nil {
			clusterAlias = ""
			logReadTopologyInstanceError(instanceKey, "DetectClusterAliasQuery", err)
		}
		instance.SuggestedClusterAlias = clusterAlias
	}
	if instance.ReplicationDepth == 0 && config.Config.DetectClusterDomainQuery != "" && !isMaxScale {
		domainName := ""
		if err := db.QueryRow(config.Config.DetectClusterDomainQuery).Scan(&domainName); err != nil {
			domainName = ""
			logReadTopologyInstanceError(instanceKey, "DetectClusterDomainQuery", err)
		}
		if domainName != "" {
			err := WriteClusterDomainName(instance.ClusterName, domainName)
			logReadTopologyInstanceError(instanceKey, "WriteClusterDomainName", err)
		}
	}

Cleanup:
	readTopologyInstanceCounter.Inc(1)
	logReadTopologyInstanceError(instanceKey, "Cleanup", err)
	if instanceFound {
		instance.IsLastCheckValid = true
		instance.IsRecentlyChecked = true
		instance.IsUpToDate = true
		_ = writeInstance(instance, instanceFound, err)
		WriteLongRunningProcesses(&instance.Key, longRunningProcesses)
		return instance, nil
	} else {
		_ = UpdateInstanceLastChecked(&instance.Key)
		return nil, fmt.Errorf("Failed ReadTopologyInstance")
	}
}

func ReadInstanceClusterAttributes(instance *Instance) (err error) {
	if config.Config.DatabaselessMode__experimental {
		return nil
	}

	var masterMasterKey InstanceKey
	var masterClusterName string
	var masterReplicationDepth uint
	masterDataFound := false

	query := `
			select
					cluster_name,
					replication_depth,
					master_host,
					master_port
				from database_instance
				where hostname=? and port=?
	`
	args := sqlutils.Args(instance.MasterKey.Hostname, instance.MasterKey.Port)

	err = db.QueryBakufu(query, args, func(m sqlutils.RowMap) error {
		masterClusterName = m.GetString("cluster_name")
		masterReplicationDepth = m.GetUint("replication_depth")
		masterMasterKey.Hostname = m.GetString("master_host")
		masterMasterKey.Port = m.GetInt("master_port")
		masterDataFound = true
		return nil
	})
	if err != nil {
		return log.Errore(err)
	}

	var replicationDepth uint = 0
	var clusterName string
	if masterDataFound {
		replicationDepth = masterReplicationDepth + 1
		clusterName = masterClusterName
	}
	clusterNameByInstanceKey := instance.Key.StringCode()
	if clusterName == "" {
		clusterName = clusterNameByInstanceKey
	}

	isCoMaster := false
	if masterMasterKey.Equals(&instance.Key) {
		isCoMaster = true
		clusterNameByCoMasterKey := instance.MasterKey.StringCode()
		if clusterName != clusterNameByInstanceKey && clusterName != clusterNameByCoMasterKey {
			log.Errorf("ReadInstanceClusterAttributes: in co-master topology %s is not in (%s, %s). Forcing it to become one of them", clusterName, clusterNameByInstanceKey, clusterNameByCoMasterKey)
			clusterName = math.TernaryString(instance.Key.SmallerThan(&instance.MasterKey), clusterNameByInstanceKey, clusterNameByCoMasterKey)
		}
		if clusterName == clusterNameByInstanceKey {
			replicationDepth = 0
		}
	}
	instance.ClusterName = clusterName
	instance.ReplicationDepth = replicationDepth
	instance.IsCoMaster = isCoMaster
	return nil
}

func ReadInstancePromotionRule(instance *Instance) (err error) {
	if config.Config.DatabaselessMode__experimental {
		return nil
	}

	var promotionRule CandidatePromotionRule = NeutralPromoteRule
	query := `
			select
					ifnull(nullif(promotion_rule, ''), 'neutral') as promotion_rule
				from candidate_database_instance
				where hostname=? and port=?
	`
	args := sqlutils.Args(instance.Key.Hostname, instance.Key.Port)

	err = db.QueryBakufu(query, args, func(m sqlutils.RowMap) error {
		promotionRule = CandidatePromotionRule(m.GetString("promotion_rule"))
		return nil
	})
	instance.PromotionRule = promotionRule
	return log.Errore(err)
}

func readInstanceRow(m sqlutils.RowMap) *Instance {
	instance := NewInstance()
	instance.Key.Hostname = m.GetString("hostname")
	instance.Key.Port = m.GetInt("port")
	instance.Uptime = m.GetUint("uptime")
	instance.ServerID = m.GetUint("server_id")
	instance.ServerUUID = m.GetString("server_uuid")
	instance.Version = m.GetString("version")
	instance.ReadOnly = m.GetBool("read_only")
	instance.Binlog_format = m.GetString("binlog_format")
	instance.LogBinEnabled = m.GetBool("log_bin")
	instance.LogSlaveUpdatesEnabled = m.GetBool("log_slave_updates")
	instance.MasterKey.Hostname = m.GetString("master_host")
	instance.MasterKey.Port = m.GetInt("master_port")
	instance.Slave_SQL_Running = m.GetBool("slave_sql_running")
	instance.Slave_IO_Running = m.GetBool("slave_io_running")
	instance.HasReplicationFilters = m.GetBool("has_replication_filters")
	instance.SupportsOracleGTID = m.GetBool("supports_oracle_gtid")
	instance.UsingOracleGTID = m.GetBool("oracle_gtid")
	instance.ExecutedGtidSet = m.GetString("executed_gtid_set")
	instance.GtidPurged = m.GetString("gtid_purged")
	instance.UsingMariaDBGTID = m.GetBool("mariadb_gtid")
	instance.UsingPseudoGTID = m.GetBool("pseudo_gtid")
	instance.SelfBinlogCoordinates.LogFile = m.GetString("binary_log_file")
	instance.SelfBinlogCoordinates.LogPos = m.GetInt64("binary_log_pos")
	instance.ReadBinlogCoordinates.LogFile = m.GetString("master_log_file")
	instance.ReadBinlogCoordinates.LogPos = m.GetInt64("read_master_log_pos")
	instance.ExecBinlogCoordinates.LogFile = m.GetString("relay_master_log_file")
	instance.ExecBinlogCoordinates.LogPos = m.GetInt64("exec_master_log_pos")
	instance.IsDetached, _, _ = instance.ExecBinlogCoordinates.DetachedCoordinates()
	instance.RelaylogCoordinates.LogFile = m.GetString("relay_log_file")
	instance.RelaylogCoordinates.LogPos = m.GetInt64("relay_log_pos")
	instance.RelaylogCoordinates.Type = RelayLog
	instance.LastSQLError = m.GetString("last_sql_error")
	instance.LastIOError = m.GetString("last_io_error")
	instance.SecondsBehindMaster = m.GetNullInt64("seconds_behind_master")
	instance.SlaveLagSeconds = m.GetNullInt64("slave_lag_seconds")
	instance.SQLDelay = m.GetUint("sql_delay")
	slaveHostsJSON := m.GetString("slave_hosts")
	instance.ClusterName = m.GetString("cluster_name")
	instance.SuggestedClusterAlias = m.GetString("suggested_cluster_alias")
	instance.DataCenter = m.GetString("data_center")
	instance.PhysicalEnvironment = m.GetString("physical_environment")
	instance.ReplicationDepth = m.GetUint("replication_depth")
	instance.IsCoMaster = m.GetBool("is_co_master")
	instance.ReplicationCredentialsAvailable = m.GetBool("replication_credentials_available")
	instance.HasReplicationCredentials = m.GetBool("has_replication_credentials")
	instance.IsUpToDate = (m.GetUint("seconds_since_last_checked") <= config.Config.InstancePollSeconds)
	instance.IsRecentlyChecked = (m.GetUint("seconds_since_last_checked") <= config.Config.InstancePollSeconds*5)
	instance.LastSeenTimestamp = m.GetString("last_seen")
	instance.IsLastCheckValid = m.GetBool("is_last_check_valid")
	instance.SecondsSinceLastSeen = m.GetNullInt64("seconds_since_last_seen")
	instance.IsCandidate = m.GetBool("is_candidate")
	instance.PromotionRule = CandidatePromotionRule(m.GetString("promotion_rule"))
	instance.IsDowntimed = m.GetBool("is_downtimed")
	instance.DowntimeReason = m.GetString("downtime_reason")
	instance.DowntimeOwner = m.GetString("downtime_owner")
	instance.DowntimeEndTimestamp = m.GetString("downtime_end_timestamp")
	instance.UnresolvedHostname = m.GetString("unresolved_hostname")
	instance.SlaveHosts.ReadJson(slaveHostsJSON)
	return instance
}

func readInstancesByCondition(condition string, args []interface{}, sort string) ([](*Instance), error) {
	readFunc := func() ([](*Instance), error) {
		instances := [](*Instance){}

		if sort == "" {
			sort = `hostname, port`
		}
		query := fmt.Sprintf(`
		select
			*,
			timestampdiff(second, last_checked, now()) as seconds_since_last_checked,
			(last_checked <= last_seen) is true as is_last_check_valid,
			timestampdiff(second, last_seen, now()) as seconds_since_last_seen,
			candidate_database_instance.last_suggested is not null
				 and candidate_database_instance.promotion_rule in ('must', 'prefer') as is_candidate,
			ifnull(nullif(candidate_database_instance.promotion_rule, ''), 'neutral') as promotion_rule,
			ifnull(unresolved_hostname, '') as unresolved_hostname,
			(
	    		database_instance_downtime.downtime_active IS NULL
	    		or database_instance_downtime.end_timestamp < NOW()
	    	) is false as is_downtimed,
	    	ifnull(database_instance_downtime.reason, '') as downtime_reason,
	    	ifnull(database_instance_downtime.owner, '') as downtime_owner,
	    	ifnull(database_instance_downtime.end_timestamp, '') as downtime_end_timestamp
		from
			database_instance
			left join candidate_database_instance using (hostname, port)
			left join hostname_unresolve using (hostname)
			left join database_instance_downtime using (hostname, port)
		where
			%s
		order by
			%s
			`, condition, sort)

		err := db.QueryBakufu(query, args, func(m sqlutils.RowMap) error {
			instance := readInstanceRow(m)
			instances = append(instances, instance)
			return nil
		})
		if err != nil {
			return instances, log.Errore(err)
		}
		err = PopulateInstancesAgents(instances)
		if err != nil {
			return instances, log.Errore(err)
		}
		return instances, err
	}
	instanceReadChan <- true
	instances, err := readFunc()
	<-instanceReadChan
	return instances, err
}

func ReadInstance(instanceKey *InstanceKey) (*Instance, bool, error) {
	if config.Config.DatabaselessMode__experimental {
		instance, err := ReadTopologyInstance(instanceKey)
		return instance, (err == nil), err
	}

	condition := `
			hostname = ?
			and port = ?
		`
	instances, err := readInstancesByCondition(condition, sqlutils.Args(instanceKey.Hostname, instanceKey.Port), "")
	readInstanceCounter.Inc(1)
	if len(instances) == 0 {
		return nil, false, err
	}
	if err != nil {
		return instances[0], false, err
	}
	return instances[0], true, nil
}

func ReadClusterInstances(clusterName string) ([](*Instance), error) {
	if strings.Index(clusterName, "'") >= 0 {
		return [](*Instance){}, log.Errorf("Invalid cluster name: %s", clusterName)
	}
	condition := `cluster_name = ?`
	return readInstancesByCondition(condition, sqlutils.Args(clusterName), "")
}

func ReadClusterWriteableMaster(clusterName string) ([](*Instance), error) {
	condition := `
		cluster_name = ?
		and read_only = 0
		and (replication_depth = 0 or is_co_master)
	`
	return readInstancesByCondition(condition, sqlutils.Args(clusterName), "replication_depth asc")
}

func ReadWriteableClustersMasters() (instances [](*Instance), err error) {
	condition := `
		read_only = 0
		and (replication_depth = 0 or is_co_master)
	`
	allMasters, err := readInstancesByCondition(condition, sqlutils.Args(), "cluster_name asc, replication_depth asc")
	if err != nil {
		return instances, err
	}
	visitedClusters := make(map[string]bool)
	for _, instance := range allMasters {
		if !visitedClusters[instance.ClusterName] {
			visitedClusters[instance.ClusterName] = true
			instances = append(instances, instance)
		}
	}
	return instances, err
}

func ReadSlaveInstances(masterKey *InstanceKey) ([](*Instance), error) {
	condition := `
			master_host = ?
			and master_port = ?
		`
	return readInstancesByCondition(condition, sqlutils.Args(masterKey.Hostname, masterKey.Port), "")
}

func ReadSlaveInstancesIncludingBinlogServerSubSlaves(masterKey *InstanceKey) ([](*Instance), error) {
	slaves, err := ReadSlaveInstances(masterKey)
	if err != nil {
		return slaves, err
	}
	for _, slave := range slaves {
		slave := slave
		if slave.IsBinlogServer() {
			binlogServerSlaves, err := ReadSlaveInstancesIncludingBinlogServerSubSlaves(&slave.Key)
			if err != nil {
				return slaves, err
			}
			slaves = append(slaves, binlogServerSlaves...)
		}
	}
	return slaves, err
}

func ReadBinlogServerSlaveInstances(masterKey *InstanceKey) ([](*Instance), error) {
	condition := `
			master_host = ?
			and master_port = ?
			and binlog_server = 1
		`
	return readInstancesByCondition(condition, sqlutils.Args(masterKey.Hostname, masterKey.Port), "")
}

func ReadUnseenInstances() ([](*Instance), error) {
	condition := `last_seen < last_checked`
	return readInstancesByCondition(condition, sqlutils.Args(), "")
}

func ReadProblemInstances(clusterName string) ([](*Instance), error) {
	condition := `
			cluster_name LIKE IF(? = '', '%', ?)
			and (
				(last_seen < last_checked)
				or (not ifnull(timestampdiff(second, last_checked, now()) <= ?, false))
				or (not slave_sql_running)
				or (not slave_io_running)
				or (abs(cast(seconds_behind_master as signed) - cast(sql_delay as signed)) > ?)
				or (abs(cast(slave_lag_seconds as signed) - cast(sql_delay as signed)) > ?)
			)
		`

	args := sqlutils.Args(clusterName, clusterName, config.Config.InstancePollSeconds, config.Config.ReasonableReplicationLagSeconds, config.Config.ReasonableReplicationLagSeconds)
	instances, err := readInstancesByCondition(condition, args, "")
	if err != nil {
		return instances, err
	}
	var reportedInstances [](*Instance)
	for _, instance := range instances {
		skip := false
		if instance.IsDowntimed {
			skip = true
		}
		for _, filter := range config.Config.ProblemIgnoreHostnameFilters {
			if matched, _ := regexp.MatchString(filter, instance.Key.Hostname); matched {
				skip = true
			}
		}
		if !skip {
			reportedInstances = append(reportedInstances, instance)
		}
	}
	return reportedInstances, nil
}

func SearchInstances(searchString string) ([](*Instance), error) {
	searchString = strings.TrimSpace(searchString)
	condition := `
			locate(?, hostname) > 0
			or locate(?, cluster_name) > 0
			or locate(?, version) > 0
			or locate(?, concat(hostname, ':', port)) > 0
			or concat(server_id, '') = ?
			or concat(port, '') = ?
		`
	args := sqlutils.Args(searchString, searchString, searchString, searchString, searchString, searchString)
	return readInstancesByCondition(condition, args, `replication_depth asc, num_slave_hosts desc, cluster_name, hostname, port`)
}

func FindInstances(regexpPattern string) ([](*Instance), error) {
	condition := `hostname rlike ?`
	return readInstancesByCondition(condition, sqlutils.Args(regexpPattern), `replication_depth asc, num_slave_hosts desc, cluster_name, hostname, port`)
}

func FindFuzzyInstances(fuzzyInstanceKey *InstanceKey) ([](*Instance), error) {
	condition := `
		hostname like concat('%', ?, '%')
		and port = ?
	`
	return readInstancesByCondition(condition, sqlutils.Args(fuzzyInstanceKey.Hostname, fuzzyInstanceKey.Port), `replication_depth asc, num_slave_hosts desc, cluster_name, hostname, port`)
}

func ReadFuzzyInstanceKey(fuzzyInstanceKey *InstanceKey) *InstanceKey {
	if fuzzyInstanceKey == nil {
		return nil
	}
	if fuzzyInstanceKey.Hostname != "" {
		if fuzzyInstances, _ := FindFuzzyInstances(fuzzyInstanceKey); len(fuzzyInstances) == 1 {
			return &(fuzzyInstances[0].Key)
		}
	}
	return nil
}

func ReadFuzzyInstanceKeyIfPossible(fuzzyInstanceKey *InstanceKey) *InstanceKey {
	if instanceKey := ReadFuzzyInstanceKey(fuzzyInstanceKey); instanceKey != nil {
		return instanceKey
	}
	return fuzzyInstanceKey
}

func ReadFuzzyInstance(fuzzyInstanceKey *InstanceKey) (*Instance, error) {
	if fuzzyInstanceKey == nil {
		return nil, log.Errorf("ReadFuzzyInstance received nil input")
	}
	if fuzzyInstanceKey.Hostname != "" {
		if fuzzyInstances, _ := FindFuzzyInstances(fuzzyInstanceKey); len(fuzzyInstances) == 1 {
			return fuzzyInstances[0], nil
		}
	}
	return nil, log.Errorf("Cannot determine fuzzy instance %+v", *fuzzyInstanceKey)
}

func ReadClusterCandidateInstances(clusterName string) ([](*Instance), error) {
	condition := `
			cluster_name = ?
			and (hostname, port) in (
				select hostname, port
					from candidate_database_instance
					where promotion_rule in ('must', 'prefer')
			)
			`
	return readInstancesByCondition(condition, sqlutils.Args(clusterName), "")
}

func filterOSCInstances(instances [](*Instance)) [](*Instance) {
	result := [](*Instance){}
	for _, instance := range instances {
		skipThisHost := false
		for _, filter := range config.Config.OSCIgnoreHostnameFilters {
			if matched, _ := regexp.MatchString(filter, instance.Key.Hostname); matched {
				skipThisHost = true
			}
		}
		if instance.IsBinlogServer() {
			skipThisHost = true
		}

		if !instance.IsLastCheckValid {
			skipThisHost = true
		}
		if !skipThisHost {
			result = append(result, instance)
		}
	}
	return result
}

func GetClusterOSCSlaves(clusterName string) ([](*Instance), error) {
	intermediateMasters := [](*Instance){}
	result := [](*Instance){}
	var err error
	if strings.Index(clusterName, "'") >= 0 {
		return [](*Instance){}, log.Errorf("Invalid cluster name: %s", clusterName)
	}
	{
		condition := `
			replication_depth = 1
			and num_slave_hosts > 0
			and cluster_name = ?
		`
		intermediateMasters, err = readInstancesByCondition(condition, sqlutils.Args(clusterName), "")
		if err != nil {
			return result, err
		}
		sort.Sort(sort.Reverse(InstancesByCountSlaveHosts(intermediateMasters)))
		intermediateMasters = filterOSCInstances(intermediateMasters)
		intermediateMasters = intermediateMasters[0:math.MinInt(2, len(intermediateMasters))]
		result = append(result, intermediateMasters...)
	}
	{
		if len(intermediateMasters) == 1 {
			slaves, err := ReadSlaveInstances(&(intermediateMasters[0].Key))
			if err != nil {
				return result, err
			}
			sort.Sort(sort.Reverse(InstancesByCountSlaveHosts(slaves)))
			slaves = filterOSCInstances(slaves)
			slaves = slaves[0:math.MinInt(2, len(slaves))]
			result = append(result, slaves...)

		}
		if len(intermediateMasters) == 2 {
			for _, im := range intermediateMasters {
				slaves, err := ReadSlaveInstances(&im.Key)
				if err != nil {
					return result, err
				}
				sort.Sort(sort.Reverse(InstancesByCountSlaveHosts(slaves)))
				slaves = filterOSCInstances(slaves)
				if len(slaves) > 0 {
					result = append(result, slaves[0])
				}
			}
		}
	}
	{
		condition := `
			replication_depth = 3
			and cluster_name = ?
		`
		slaves, err := readInstancesByCondition(condition, sqlutils.Args(clusterName), "")
		if err != nil {
			return result, err
		}
		sort.Sort(sort.Reverse(InstancesByCountSlaveHosts(slaves)))
		slaves = filterOSCInstances(slaves)
		slaves = slaves[0:math.MinInt(2, len(slaves))]
		result = append(result, slaves...)
	}
	{
		condition := `
			replication_depth = 1
			and num_slave_hosts = 0
			and cluster_name = ?
		`
		slaves, err := readInstancesByCondition(condition, sqlutils.Args(clusterName), "")
		if err != nil {
			return result, err
		}
		slaves = filterOSCInstances(slaves)
		slaves = slaves[0:math.MinInt(2, len(slaves))]
		result = append(result, slaves...)
	}

	return result, nil
}

func GetInstancesMaxLag(instances [](*Instance)) (maxLag int64, err error) {
	if len(instances) == 0 {
		return 0, log.Errorf("No instances found in GetInstancesMaxLag")
	}
	for _, clusterInstance := range instances {
		if clusterInstance.SlaveLagSeconds.Valid && clusterInstance.SlaveLagSeconds.Int64 > maxLag {
			maxLag = clusterInstance.SlaveLagSeconds.Int64
		}
	}
	return maxLag, nil
}

func GetClusterHeuristicLag(clusterName string) (int64, error) {
	instances, err := GetClusterOSCSlaves(clusterName)
	if err != nil {
		return 0, err
	}
	return GetInstancesMaxLag(instances)
}

func GetHeuristicClusterPoolInstances(clusterName string, pool string) (result [](*Instance), err error) {
	instances, err := ReadClusterInstances(clusterName)
	if err != nil {
		return result, err
	}

	pooledInstanceKeys := NewInstanceKeyMap()
	clusterPoolInstances, err := ReadClusterPoolInstances(clusterName, pool)
	if err != nil {
		return result, err
	}
	for _, clusterPoolInstance := range clusterPoolInstances {
		pooledInstanceKeys.AddKey(InstanceKey{Hostname: clusterPoolInstance.Hostname, Port: clusterPoolInstance.Port})
	}

	for _, instance := range instances {
		skipThisHost := false
		if instance.IsBinlogServer() {
			skipThisHost = true
		}
		if !instance.IsLastCheckValid {
			skipThisHost = true
		}
		if !pooledInstanceKeys.HasKey(instance.Key) {
			skipThisHost = true
		}
		if !skipThisHost {
			result = append(result, instance)
		}
	}
	return result, err
}

func GetHeuristicClusterPoolInstancesLag(clusterName string, pool string) (int64, error) {
	instances, err := GetHeuristicClusterPoolInstances(clusterName, pool)
	if err != nil {
		return 0, err
	}
	return GetInstancesMaxLag(instances)
}

func updateInstanceClusterName(instance *Instance) error {
	writeFunc := func() error {
		_, err := db.ExecBakufu(`
			update
				database_instance
			set
				cluster_name=?
			where
				hostname=? and port=?
        	`, instance.ClusterName, instance.Key.Hostname, instance.Key.Port,
		)
		if err != nil {
			return log.Errore(err)
		}
		AuditOperation("update-cluster-name", &instance.Key, fmt.Sprintf("set to %s", instance.ClusterName))
		return nil
	}
	return ExecDBWriteFunc(writeFunc)
}

func ReviewUnseenInstances() error {
	instances, err := ReadUnseenInstances()
	if err != nil {
		return log.Errore(err)
	}
	operations := 0
	for _, instance := range instances {
		instance := instance

		masterHostname, err := ResolveHostname(instance.MasterKey.Hostname)
		if err != nil {
			log.Errore(err)
			continue
		}
		instance.MasterKey.Hostname = masterHostname
		savedClusterName := instance.ClusterName

		if err := ReadInstanceClusterAttributes(instance); err != nil {
			log.Errore(err)
		} else if instance.ClusterName != savedClusterName {
			updateInstanceClusterName(instance)
			operations++
		}
	}

	AuditOperation("review-unseen-instances", nil, fmt.Sprintf("Operations: %d", operations))
	return err
}

func readUnseenMasterKeys() ([]InstanceKey, error) {
	res := []InstanceKey{}

	err := db.QueryBakufuRowsMap(`
			SELECT DISTINCT
			    slave_instance.master_host, slave_instance.master_port
			FROM
			    database_instance slave_instance
			        LEFT JOIN
			    hostname_resolve ON (slave_instance.master_host = hostname_resolve.hostname)
			        LEFT JOIN
			    database_instance master_instance ON (
			    	COALESCE(hostname_resolve.resolved_hostname, slave_instance.master_host) = master_instance.hostname
			    	and slave_instance.master_port = master_instance.port)
			WHERE
			    master_instance.last_checked IS NULL
			    and slave_instance.master_host != ''
			    and slave_instance.master_host != '_'
			    and slave_instance.master_port > 0
			    and slave_instance.slave_io_running = 1
			`, func(m sqlutils.RowMap) error {
		instanceKey, _ := NewInstanceKeyFromStrings(m.GetString("master_host"), m.GetString("master_port"))
		res = append(res, *instanceKey)

		return nil
	})
	if err != nil {
		return res, log.Errore(err)
	}

	return res, nil
}

func InjectUnseenMasters() error {

	unseenMasterKeys, err := readUnseenMasterKeys()
	if err != nil {
		return err
	}

	operations := 0
	for _, masterKey := range unseenMasterKeys {
		masterKey := masterKey
		clusterName := masterKey.StringCode()
		instance := Instance{Key: masterKey, Version: "Unknown", ClusterName: clusterName}
		if err := writeInstance(&instance, false, nil); err == nil {
			operations++
		}
	}

	AuditOperation("inject-unseen-masters", nil, fmt.Sprintf("Operations: %d", operations))
	return err
}

func ForgetUnseenInstancesDifferentlyResolved() error {
	sqlResult, err := db.ExecBakufu(`
		DELETE FROM
			database_instance
		USING
		    hostname_resolve
		    JOIN database_instance ON (hostname_resolve.hostname = database_instance.hostname)
		WHERE
		    hostname_resolve.hostname != hostname_resolve.resolved_hostname
		    AND (last_checked <= last_seen) IS NOT TRUE
		`,
	)
	if err != nil {
		return log.Errore(err)
	}
	rows, err := sqlResult.RowsAffected()
	if err != nil {
		return log.Errore(err)
	}
	AuditOperation("forget-unseen-differently-resolved", nil, fmt.Sprintf("Forgotten instances: %d", rows))
	return err
}

func readUnknownMasterHostnameResolves() (map[string]string, error) {
	res := make(map[string]string)
	err := db.QueryBakufuRowsMap(`
			SELECT DISTINCT
			    slave_instance.master_host, hostname_resolve_history.resolved_hostname
			FROM
			    database_instance slave_instance
			LEFT JOIN hostname_resolve ON (slave_instance.master_host = hostname_resolve.hostname)
			LEFT JOIN database_instance master_instance ON (
			    COALESCE(hostname_resolve.resolved_hostname, slave_instance.master_host) = master_instance.hostname
			    and slave_instance.master_port = master_instance.port
			) LEFT JOIN hostname_resolve_history ON (slave_instance.master_host = hostname_resolve_history.hostname)
			WHERE
			    master_instance.last_checked IS NULL
			    and slave_instance.master_host != ''
			    and slave_instance.master_host != '_'
			    and slave_instance.master_port > 0
			`, func(m sqlutils.RowMap) error {
		res[m.GetString("master_host")] = m.GetString("resolved_hostname")
		return nil
	})
	if err != nil {
		return res, log.Errore(err)
	}

	return res, nil
}

func ResolveUnknownMasterHostnameResolves() error {

	hostnameResolves, err := readUnknownMasterHostnameResolves()
	if err != nil {
		return err
	}
	for hostname, resolvedHostname := range hostnameResolves {
		UpdateResolvedHostname(hostname, resolvedHostname)
	}

	AuditOperation("resolve-unknown-masters", nil, fmt.Sprintf("Num resolved hostnames: %d", len(hostnameResolves)))
	return err
}

func ReadCountMySQLSnapshots(hostnames []string) (map[string]int, error) {
	res := make(map[string]int)
	if !config.Config.ServeAgentsHttp {
		return res, nil
	}
	query := fmt.Sprintf(`
		select
			hostname,
			count_mysql_snapshots
		from
			host_agent
		where
			hostname in (%s)
		order by
			hostname
		`, sqlutils.InClauseStringValues(hostnames))

	err := db.QueryBakufuRowsMap(query, func(m sqlutils.RowMap) error {
		res[m.GetString("hostname")] = m.GetInt("count_mysql_snapshots")
		return nil
	})

	if err != nil {
		log.Errore(err)
	}
	return res, err
}

func PopulateInstancesAgents(instances [](*Instance)) error {
	if len(instances) == 0 {
		return nil
	}
	hostnames := []string{}
	for _, instance := range instances {
		hostnames = append(hostnames, instance.Key.Hostname)
	}
	agentsCountMySQLSnapshots, err := ReadCountMySQLSnapshots(hostnames)
	if err != nil {
		return err
	}
	for _, instance := range instances {
		if count, ok := agentsCountMySQLSnapshots[instance.Key.Hostname]; ok {
			instance.CountMySQLSnapshots = count
		}
	}

	return nil
}

func GetClusterName(instanceKey *InstanceKey) (clusterName string, err error) {
	if clusterName, found := instanceKeyInformativeClusterName.Get(instanceKey.DisplayString()); found {
		return clusterName.(string), nil
	}
	query := `
		select
			ifnull(max(cluster_name), '') as cluster_name
		from
			database_instance
		where
			hostname = ?
			and port = ?
			`
	err = db.QueryBakufu(query, sqlutils.Args(instanceKey.Hostname, instanceKey.Port), func(m sqlutils.RowMap) error {
		clusterName = m.GetString("cluster_name")
		instanceKeyInformativeClusterName.Set(instanceKey.DisplayString(), clusterName, cache.DefaultExpiration)
		return nil
	})

	return clusterName, log.Errore(err)
}

func ReadClusters() ([]string, error) {
	clusterNames := []string{}

	query := `
		select
			cluster_name
		from
			database_instance
		group by
			cluster_name`

	err := db.QueryBakufuRowsMap(query, func(m sqlutils.RowMap) error {
		clusterNames = append(clusterNames, m.GetString("cluster_name"))
		return nil
	})

	return clusterNames, log.Errore(err)
}

func ReadClusterInfo(clusterName string) (*ClusterInfo, error) {
	clusterInfo := &ClusterInfo{}

	query := `
		select
			cluster_name,
			count(*) as count_instances
		from
			database_instance
		where
			cluster_name=?
		group by
			cluster_name`

	err := db.QueryBakufu(query, sqlutils.Args(clusterName), func(m sqlutils.RowMap) error {
		clusterInfo.ClusterName = m.GetString("cluster_name")
		clusterInfo.CountInstances = m.GetUint("count_instances")
		ApplyClusterAlias(clusterInfo)
		ApplyClusterDomain(clusterInfo)
		clusterInfo.ReadRecoveryInfo()
		return nil
	})
	if err != nil {
		return clusterInfo, err
	}
	clusterInfo.HeuristicLag, err = GetClusterHeuristicLag(clusterName)

	return clusterInfo, err
}

func ReadClustersInfo() ([]ClusterInfo, error) {
	clusters := []ClusterInfo{}

	query := `
		select
			cluster_name,
			count(*) as count_instances
		from
			database_instance
		group by
			cluster_name`

	err := db.QueryBakufuRowsMap(query, func(m sqlutils.RowMap) error {
		clusterInfo := ClusterInfo{
			ClusterName:    m.GetString("cluster_name"),
			CountInstances: m.GetUint("count_instances"),
		}
		ApplyClusterAlias(&clusterInfo)
		ApplyClusterDomain(&clusterInfo)
		clusterInfo.ReadRecoveryInfo()

		clusters = append(clusters, clusterInfo)
		return nil
	})

	return clusters, err
}

func ReadOutdatedInstanceKeys() ([]InstanceKey, error) {
	res := []InstanceKey{}
	query := `
		select
			hostname, port
		from
			database_instance
		where
			if (
				last_attempted_check <= last_checked,
				last_checked < now() - interval ? second,
				last_checked < now() - interval (? * 2) second
			)
			`
	args := sqlutils.Args(config.Config.InstancePollSeconds, config.Config.InstancePollSeconds)

	err := db.QueryBakufu(query, args, func(m sqlutils.RowMap) error {
		instanceKey, merr := NewInstanceKeyFromStrings(m.GetString("hostname"), m.GetString("port"))
		if merr != nil {
			log.Errore(merr)
		} else {
			res = append(res, *instanceKey)
		}
		return nil
	})

	if err != nil {
		log.Errore(err)
	}
	return res, err

}

func writeInstance(instance *Instance, instanceWasActuallyFound bool, lastError error) error {

	writeFunc := func() error {
		insertIgnore := ""
		onDuplicateKeyUpdate := ""
		if instanceWasActuallyFound {
			onDuplicateKeyUpdate = `
				on duplicate key update
      		last_checked=VALUES(last_checked),
      		last_attempted_check=VALUES(last_attempted_check),
      		uptime=VALUES(uptime),
      		server_id=VALUES(server_id),
      		server_uuid=VALUES(server_uuid),
					version=VALUES(version),
					binlog_server=VALUES(binlog_server),
					read_only=VALUES(read_only),
					binlog_format=VALUES(binlog_format),
					log_bin=VALUES(log_bin),
					log_slave_updates=VALUES(log_slave_updates),
					binary_log_file=VALUES(binary_log_file),
					binary_log_pos=VALUES(binary_log_pos),
					master_host=VALUES(master_host),
					master_port=VALUES(master_port),
					slave_sql_running=VALUES(slave_sql_running),
					slave_io_running=VALUES(slave_io_running),
					has_replication_filters=VALUES(has_replication_filters),
					supports_oracle_gtid=VALUES(supports_oracle_gtid),
					oracle_gtid=VALUES(oracle_gtid),
					executed_gtid_set=VALUES(executed_gtid_set),
					gtid_purged=VALUES(gtid_purged),
					mariadb_gtid=VALUES(mariadb_gtid),
					pseudo_gtid=values(pseudo_gtid),
					master_log_file=VALUES(master_log_file),
					read_master_log_pos=VALUES(read_master_log_pos),
					relay_master_log_file=VALUES(relay_master_log_file),
					exec_master_log_pos=VALUES(exec_master_log_pos),
					relay_log_file=VALUES(relay_log_file),
					relay_log_pos=VALUES(relay_log_pos),
					last_sql_error=VALUES(last_sql_error),
					last_io_error=VALUES(last_io_error),
					seconds_behind_master=VALUES(seconds_behind_master),
					slave_lag_seconds=VALUES(slave_lag_seconds),
					sql_delay=VALUES(sql_delay),
					num_slave_hosts=VALUES(num_slave_hosts),
					slave_hosts=VALUES(slave_hosts),
					cluster_name=VALUES(cluster_name),
					suggested_cluster_alias=VALUES(suggested_cluster_alias),
					data_center=VALUES(data_center),
					physical_environment=values(physical_environment),
					replication_depth=VALUES(replication_depth),
					is_co_master=VALUES(is_co_master),
					replication_credentials_available=VALUES(replication_credentials_available),
					has_replication_credentials=VALUES(has_replication_credentials)
				`
		} else {
			insertIgnore = `ignore`
		}
		insertQuery := fmt.Sprintf(`
    	insert %s into database_instance (
    		hostname,
    		port,
    		last_checked,
    		last_attempted_check,
    		uptime,
    		server_id,
    		server_uuid,
				version,
				binlog_server,
				read_only,
				binlog_format,
				log_bin,
				log_slave_updates,
				binary_log_file,
				binary_log_pos,
				master_host,
				master_port,
				slave_sql_running,
				slave_io_running,
				has_replication_filters,
				supports_oracle_gtid,
				oracle_gtid,
				executed_gtid_set,
				gtid_purged,
				mariadb_gtid,
				pseudo_gtid,
				master_log_file,
				read_master_log_pos,
				relay_master_log_file,
				exec_master_log_pos,
				relay_log_file,
				relay_log_pos,
				last_sql_error,
				last_io_error,
				seconds_behind_master,
				slave_lag_seconds,
				sql_delay,
				num_slave_hosts,
				slave_hosts,
				cluster_name,
				suggested_cluster_alias,
				data_center,
				physical_environment,
				replication_depth,
				is_co_master,
				replication_credentials_available,
				has_replication_credentials
			) values (?, ?, NOW(), NOW(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			%s
			`, insertIgnore, onDuplicateKeyUpdate)

		_, err := db.ExecBakufu(insertQuery,
			instance.Key.Hostname,
			instance.Key.Port,
			instance.Uptime,
			instance.ServerID,
			instance.ServerUUID,
			instance.Version,
			instance.IsBinlogServer(),
			instance.ReadOnly,
			instance.Binlog_format,
			instance.LogBinEnabled,
			instance.LogSlaveUpdatesEnabled,
			instance.SelfBinlogCoordinates.LogFile,
			instance.SelfBinlogCoordinates.LogPos,
			instance.MasterKey.Hostname,
			instance.MasterKey.Port,
			instance.Slave_SQL_Running,
			instance.Slave_IO_Running,
			instance.HasReplicationFilters,
			instance.SupportsOracleGTID,
			instance.UsingOracleGTID,
			instance.ExecutedGtidSet,
			instance.GtidPurged,
			instance.UsingMariaDBGTID,
			instance.UsingPseudoGTID,
			instance.ReadBinlogCoordinates.LogFile,
			instance.ReadBinlogCoordinates.LogPos,
			instance.ExecBinlogCoordinates.LogFile,
			instance.ExecBinlogCoordinates.LogPos,
			instance.RelaylogCoordinates.LogFile,
			instance.RelaylogCoordinates.LogPos,
			instance.LastSQLError,
			instance.LastIOError,
			instance.SecondsBehindMaster,
			instance.SlaveLagSeconds,
			instance.SQLDelay,
			len(instance.SlaveHosts),
			instance.SlaveHosts.ToJSONString(),
			instance.ClusterName,
			instance.SuggestedClusterAlias,
			instance.DataCenter,
			instance.PhysicalEnvironment,
			instance.ReplicationDepth,
			instance.IsCoMaster,
			instance.ReplicationCredentialsAvailable,
			instance.HasReplicationCredentials,
		)
		if err != nil {
			return log.Errore(err)
		}

		if instanceWasActuallyFound && lastError == nil {
			db.ExecBakufu(`
        		update database_instance set last_seen = NOW() where hostname=? and port=?
        	`, instance.Key.Hostname, instance.Key.Port,
			)
		} else {
			log.Debugf("writeInstance: will not update database_instance due to error: %+v", lastError)
		}
		writeInstanceCounter.Inc(1)
		return nil
	}
	return ExecDBWriteFunc(writeFunc)
}

func UpdateInstanceLastChecked(instanceKey *InstanceKey) error {
	writeFunc := func() error {
		_, err := db.ExecBakufu(`
        	update
        		database_instance
        	set
        		last_checked = NOW()
			where
				hostname = ?
				and port = ?`,
			instanceKey.Hostname,
			instanceKey.Port,
		)
		if err != nil {
			return log.Errore(err)
		}

		return nil
	}
	return ExecDBWriteFunc(writeFunc)
}

func UpdateInstanceLastAttemptedCheck(instanceKey *InstanceKey) error {
	writeFunc := func() error {
		_, err := db.ExecBakufu(`
        	update
        		database_instance
        	set
        		last_attempted_check = NOW()
			where
				hostname = ?
				and port = ?`,
			instanceKey.Hostname,
			instanceKey.Port,
		)
		if err != nil {
			return log.Errore(err)
		}

		return nil
	}
	return ExecDBWriteFunc(writeFunc)
}

func ForgetInstance(instanceKey *InstanceKey) error {
	_, err := db.ExecBakufu(`
			delete
				from database_instance
			where
				hostname = ? and port = ?`,
		instanceKey.Hostname,
		instanceKey.Port,
	)
	AuditOperation("forget", instanceKey, "")
	return err
}

func ForgetLongUnseenInstances() error {
	sqlResult, err := db.ExecBakufu(`
			delete
				from database_instance
			where
				last_seen < NOW() - interval ? hour`,
		config.Config.UnseenInstanceForgetHours,
	)
	if err != nil {
		return log.Errore(err)
	}
	rows, err := sqlResult.RowsAffected()
	if err != nil {
		return log.Errore(err)
	}
	AuditOperation("forget-unseen", nil, fmt.Sprintf("Forgotten instances: %d", rows))
	return err
}

func SnapshotTopologies() error {
	writeFunc := func() error {
		_, err := db.ExecBakufu(`
        	insert ignore into
        		database_instance_topology_history (snapshot_unix_timestamp,
        			hostname, port, master_host, master_port, cluster_name, version)
        	select
        		UNIX_TIMESTAMP(NOW()),
        		hostname, port, master_host, master_port, cluster_name, version
			from
				database_instance
				`,
		)
		if err != nil {
			return log.Errore(err)
		}

		return nil
	}
	return ExecDBWriteFunc(writeFunc)
}

func ReadHistoryClusterInstances(clusterName string, historyTimestampPattern string) ([](*Instance), error) {
	instances := [](*Instance){}

	query := `
		select
			*
		from
			database_instance_topology_history
		where
			snapshot_unix_timestamp rlike ?
			and cluster_name = ?
		order by
			hostname, port`

	err := db.QueryBakufu(query, sqlutils.Args(historyTimestampPattern, clusterName), func(m sqlutils.RowMap) error {
		instance := NewInstance()

		instance.Key.Hostname = m.GetString("hostname")
		instance.Key.Port = m.GetInt("port")
		instance.MasterKey.Hostname = m.GetString("master_host")
		instance.MasterKey.Port = m.GetInt("master_port")
		instance.ClusterName = m.GetString("cluster_name")

		instances = append(instances, instance)
		return nil
	})
	if err != nil {
		return instances, log.Errore(err)
	}
	return instances, err
}

func RegisterCandidateInstance(instanceKey *InstanceKey, promotionRule CandidatePromotionRule) error {
	writeFunc := func() error {
		_, err := db.ExecBakufu(`
				insert into candidate_database_instance (
						hostname,
						port,
						last_suggested,
						promotion_rule)
					values (?, ?, NOW(), ?)
					on duplicate key update
						hostname=values(hostname),
						port=values(port),
						last_suggested=now(),
						promotion_rule=values(promotion_rule)
				`, instanceKey.Hostname, instanceKey.Port, string(promotionRule),
		)
		if err != nil {
			return log.Errore(err)
		}

		return nil
	}
	return ExecDBWriteFunc(writeFunc)
}

func ExpireCandidateInstances() error {
	writeFunc := func() error {
		_, err := db.ExecBakufu(`
        	delete from candidate_database_instance
				where last_suggested < NOW() - INTERVAL ? MINUTE
				`, config.Config.CandidateInstanceExpireMinutes,
		)
		if err != nil {
			return log.Errore(err)
		}

		return nil
	}
	return ExecDBWriteFunc(writeFunc)
}

func RecordInstanceCoordinatesHistory() error {
	{
		writeFunc := func() error {
			_, err := db.ExecBakufu(`
        	delete from database_instance_coordinates_history
			where
				recorded_timestamp < NOW() - INTERVAL (? + 5) MINUTE
				`, config.Config.PseudoGTIDCoordinatesHistoryHeuristicMinutes,
			)
			return log.Errore(err)
		}
		ExecDBWriteFunc(writeFunc)
	}
	writeFunc := func() error {
		_, err := db.ExecBakufu(`
        	insert into
        		database_instance_coordinates_history (
					hostname, port,	last_seen, recorded_timestamp,
					binary_log_file, binary_log_pos, relay_log_file, relay_log_pos
				)
        	select
        		hostname, port, last_seen, NOW(),
				binary_log_file, binary_log_pos, relay_log_file, relay_log_pos
			from
				database_instance
			where
				binary_log_file != ''
				OR relay_log_file != ''
				`,
		)
		return log.Errore(err)
	}
	return ExecDBWriteFunc(writeFunc)
}

func GetHeuristiclyRecentCoordinatesForInstance(instanceKey *InstanceKey) (selfCoordinates *BinlogCoordinates, relayLogCoordinates *BinlogCoordinates, err error) {
	query := `
		select
			binary_log_file, binary_log_pos, relay_log_file, relay_log_pos
		from
			database_instance_coordinates_history
		where
			hostname = ?
			and port = ?
			and recorded_timestamp <= NOW() - INTERVAL ? MINUTE
		order by
			recorded_timestamp desc
			limit 1
			`
	err = db.QueryBakufu(query, sqlutils.Args(instanceKey.Hostname, instanceKey.Port, config.Config.PseudoGTIDCoordinatesHistoryHeuristicMinutes), func(m sqlutils.RowMap) error {
		selfCoordinates = &BinlogCoordinates{LogFile: m.GetString("binary_log_file"), LogPos: m.GetInt64("binary_log_pos")}
		relayLogCoordinates = &BinlogCoordinates{LogFile: m.GetString("relay_log_file"), LogPos: m.GetInt64("relay_log_pos")}

		return nil
	})
	return selfCoordinates, relayLogCoordinates, err
}

func RecordInstanceBinlogFileHistory() error {
	{
		writeFunc := func() error {
			_, err := db.ExecBakufu(`
        	delete from database_instance_binlog_files_history
			where
				last_seen < NOW() - INTERVAL ? DAY
				`, config.Config.BinlogFileHistoryDays,
			)
			return log.Errore(err)
		}
		ExecDBWriteFunc(writeFunc)
	}
	if config.Config.BinlogFileHistoryDays == 0 {
		return nil
	}
	writeFunc := func() error {
		_, err := db.ExecBakufu(`
      	insert into
      		database_instance_binlog_files_history (
					hostname, port,	first_seen, last_seen, binary_log_file, binary_log_pos
				)
      	select
      		hostname, port, last_seen, last_seen, binary_log_file, binary_log_pos
				from
					database_instance
				where
					binary_log_file != ''
				on duplicate key update
					last_seen = VALUES(last_seen),
					binary_log_pos = VALUES(binary_log_pos)
				`,
		)
		return log.Errore(err)
	}
	return ExecDBWriteFunc(writeFunc)
}

func UpdateInstanceRecentRelaylogHistory() error {
	writeFunc := func() error {
		_, err := db.ExecBakufu(`
        	insert into
        		database_instance_recent_relaylog_history (
							hostname, port,
							current_relay_log_file, current_relay_log_pos, current_seen,
							prev_relay_log_file, prev_relay_log_pos
						)
						select
								hostname, port,
								relay_log_file, relay_log_pos, last_seen,
								'', 0
							from database_instance
							where
								relay_log_file != ''
						on duplicate key update
							prev_relay_log_file = current_relay_log_file,
							prev_relay_log_pos = current_relay_log_pos,
							prev_seen = current_seen,
							current_relay_log_file = values(current_relay_log_file),
							current_relay_log_pos = values (current_relay_log_pos),
							current_seen = values(current_seen)
				`,
		)
		return log.Errore(err)
	}
	return ExecDBWriteFunc(writeFunc)
}
