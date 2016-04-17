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
	"github.com/pmylund/go-cache"
	"github.com/rcrowley/go-metrics"
	"regexp"
	"time"
)

var analysisChangeWriteAttemptCounter = metrics.NewCounter()
var analysisChangeWriteCounter = metrics.NewCounter()

func init() {
	metrics.Register("analysis.change.write.attempt", analysisChangeWriteAttemptCounter)
	metrics.Register("analysis.change.write", analysisChangeWriteCounter)
}

var recentInstantAnalysis = cache.New(time.Duration(config.Config.RecoveryPollSeconds*2)*time.Second, time.Second)

func GetReplicationAnalysis(clusterName string, includeDowntimed bool, auditAnalysis bool) ([]ReplicationAnalysis, error) {
	result := []ReplicationAnalysis{}

	args := sqlutils.Args(config.Config.InstancePollSeconds, clusterName)
	analysisQueryReductionClause := ``
	if config.Config.ReduceReplicationAnalysisCount {
		analysisQueryReductionClause = `
			HAVING
				(MIN(
		        		master_instance.last_checked <= master_instance.last_seen
		        		AND master_instance.last_attempted_check <= master_instance.last_seen + INTERVAL (2 * ?) SECOND
		        	) IS TRUE /* AS is_last_check_valid */) = 0
				OR (IFNULL(SUM(slave_instance.last_checked <= slave_instance.last_seen
		                    AND slave_instance.slave_io_running = 0
		                    AND slave_instance.last_io_error RLIKE 'error (connecting|reconnecting) to master'
		                    AND slave_instance.slave_sql_running = 1),
		                0) /* AS count_slaves_failing_to_connect_to_master */ > 0)
				OR (IFNULL(SUM(slave_instance.last_checked <= slave_instance.last_seen),
		                0) /* AS count_valid_slaves */ < COUNT(slave_instance.server_id) /* AS count_slaves */)
				OR (IFNULL(SUM(slave_instance.last_checked <= slave_instance.last_seen
		                    AND slave_instance.slave_io_running != 0
		                    AND slave_instance.slave_sql_running != 0),
		                0) /* AS count_valid_replicating_slaves */ < COUNT(slave_instance.server_id) /* AS count_slaves */)
				OR (MIN(
		            master_instance.slave_sql_running = 1
		            AND master_instance.slave_io_running = 0
		            AND master_instance.last_io_error RLIKE 'error (connecting|reconnecting) to master'
		          ) /* AS is_failing_to_connect_to_master */)
				OR (COUNT(slave_instance.server_id) /* AS count_slaves */ > 0)
			`
		args = append(args, config.Config.InstancePollSeconds)
	}
	query := fmt.Sprintf(`
		    SELECT
		        master_instance.hostname,
		        master_instance.port,
		        MIN(master_instance.master_host) AS master_host,
		        MIN(master_instance.master_port) AS master_port,
		        MIN(master_instance.cluster_name) AS cluster_name,
		        MIN(IFNULL(cluster_alias.alias, master_instance.cluster_name)) AS cluster_alias,
		        MIN(
		        		master_instance.last_checked <= master_instance.last_seen
		        		AND master_instance.last_attempted_check <= master_instance.last_seen + INTERVAL (2 * ?) SECOND
		        	) IS TRUE AS is_last_check_valid,
		        MIN(master_instance.master_host IN ('' , '_')
		            OR master_instance.master_port = 0) AS is_master,
		        MIN(master_instance.is_co_master) AS is_co_master,
		        MIN(CONCAT(master_instance.hostname,
		                ':',
		                master_instance.port) = master_instance.cluster_name) AS is_cluster_master,
		        COUNT(slave_instance.server_id) AS count_slaves,
		        IFNULL(SUM(slave_instance.last_checked <= slave_instance.last_seen),
		                0) AS count_valid_slaves,
		        IFNULL(SUM(slave_instance.last_checked <= slave_instance.last_seen
		                    AND slave_instance.slave_io_running != 0
		                    AND slave_instance.slave_sql_running != 0),
		                0) AS count_valid_replicating_slaves,
		        IFNULL(SUM(slave_instance.last_checked <= slave_instance.last_seen
		                    AND slave_instance.slave_io_running = 0
		                    AND slave_instance.last_io_error RLIKE 'error (connecting|reconnecting) to master'
		                    AND slave_instance.slave_sql_running = 1),
		                0) AS count_slaves_failing_to_connect_to_master,
						IFNULL(SUM(
									current_relay_log_file=prev_relay_log_file
									and current_relay_log_pos=prev_relay_log_pos
									and current_seen != prev_seen),
								0) AS count_stale_slaves,
		        MIN(master_instance.replication_depth) AS replication_depth,
		        GROUP_CONCAT(slave_instance.Hostname, ':', slave_instance.Port) as slave_hosts,
		        MIN(
		            master_instance.slave_sql_running = 1
		            AND master_instance.slave_io_running = 0
		            AND master_instance.last_io_error RLIKE 'error (connecting|reconnecting) to master'
		          ) AS is_failing_to_connect_to_master,
		        MIN(
				    		database_instance_downtime.downtime_active IS NULL
				    		OR database_instance_downtime.end_timestamp < NOW()
			    		) IS FALSE AS is_downtimed,
			    	MIN(
				    		IFNULL(database_instance_downtime.end_timestamp, '')
				    	) AS downtime_end_timestamp,
			    	MIN(
				    		IFNULL(TIMESTAMPDIFF(SECOND, NOW(), database_instance_downtime.end_timestamp), 0)
				    	) AS downtime_remaining_seconds,
			    	MIN(
				    		master_instance.binlog_server
				    	) AS is_binlog_server,
			    	MIN(
				    		master_instance.pseudo_gtid
				    	) AS is_pseudo_gtid,
			    	MIN(
				    		master_instance.supports_oracle_gtid
				    	) AS supports_oracle_gtid,
			    	SUM(
				    		slave_instance.oracle_gtid
				    	) AS count_oracle_gtid_slaves,
			      IFNULL(SUM(slave_instance.last_checked <= slave_instance.last_seen
	              AND slave_instance.oracle_gtid != 0),
              0) AS count_valid_oracle_gtid_slaves,
			    	SUM(
				    		slave_instance.binlog_server
				    	) AS count_binlog_server_slaves,
		        IFNULL(SUM(slave_instance.last_checked <= slave_instance.last_seen
                  AND slave_instance.binlog_server != 0),
              0) AS count_valid_binlog_server_slaves,
			    	MIN(
				    		master_instance.mariadb_gtid
				    	) AS is_mariadb_gtid,
			    	SUM(
				    		slave_instance.mariadb_gtid
				    	) AS count_mariadb_gtid_slaves,
		        IFNULL(SUM(slave_instance.last_checked <= slave_instance.last_seen
                  AND slave_instance.mariadb_gtid != 0),
              0) AS count_valid_mariadb_gtid_slaves,
						IFNULL(SUM(slave_instance.log_bin
							  AND slave_instance.log_slave_updates
								AND slave_instance.binlog_format = 'STATEMENT'),
              0) AS count_statement_based_loggin_slaves,
						IFNULL(SUM(slave_instance.log_bin
								AND slave_instance.log_slave_updates
								AND slave_instance.binlog_format = 'MIXED'),
              0) AS count_mixed_based_loggin_slaves,
						IFNULL(SUM(slave_instance.log_bin
								AND slave_instance.log_slave_updates
								AND slave_instance.binlog_format = 'ROW'),
              0) AS count_row_based_loggin_slaves,
						COUNT(DISTINCT IF(
							slave_instance.log_bin AND slave_instance.log_slave_updates,
								substring_index(slave_instance.version, '.', 2),
								NULL)
						) AS count_distinct_logging_major_versions
		    FROM
		        database_instance master_instance
		            LEFT JOIN
		        hostname_resolve ON (master_instance.hostname = hostname_resolve.hostname)
		            LEFT JOIN
		        database_instance slave_instance ON (COALESCE(hostname_resolve.resolved_hostname,
		                master_instance.hostname) = slave_instance.master_host
		            	AND master_instance.port = slave_instance.master_port)
		            LEFT JOIN
		        database_instance_maintenance ON (master_instance.hostname = database_instance_maintenance.hostname
		        		AND master_instance.port = database_instance_maintenance.port
		        		AND database_instance_maintenance.maintenance_active = 1)
		            LEFT JOIN
		        database_instance_downtime ON (master_instance.hostname = database_instance_downtime.hostname
		        		AND master_instance.port = database_instance_downtime.port
		        		AND database_instance_downtime.downtime_active = 1)
		        	LEFT JOIN
		        cluster_alias ON (cluster_alias.cluster_name = master_instance.cluster_name)
						  LEFT JOIN
						database_instance_recent_relaylog_history ON (
								slave_instance.hostname = database_instance_recent_relaylog_history.hostname
		        		AND slave_instance.port = database_instance_recent_relaylog_history.port)
		    WHERE
		    	database_instance_maintenance.database_instance_maintenance_id IS NULL
		    	AND ? IN ('', master_instance.cluster_name)
		    GROUP BY
			    master_instance.hostname,
			    master_instance.port
			%s
		    ORDER BY
			    is_master DESC ,
			    is_cluster_master DESC,
			    count_slaves DESC
	`, analysisQueryReductionClause)
	err := db.QueryBakufu(query, args, func(m sqlutils.RowMap) error {
		a := ReplicationAnalysis{Analysis: NoProblem}

		a.IsMaster = m.GetBool("is_master")
		a.IsCoMaster = m.GetBool("is_co_master")
		a.AnalyzedInstanceKey = InstanceKey{Hostname: m.GetString("hostname"), Port: m.GetInt("port")}
		a.AnalyzedInstanceMasterKey = InstanceKey{Hostname: m.GetString("master_host"), Port: m.GetInt("master_port")}
		a.ClusterDetails.ClusterName = m.GetString("cluster_name")
		a.ClusterDetails.ClusterAlias = m.GetString("cluster_alias")
		a.LastCheckValid = m.GetBool("is_last_check_valid")
		a.CountSlaves = m.GetUint("count_slaves")
		a.CountValidSlaves = m.GetUint("count_valid_slaves")
		a.CountValidReplicatingSlaves = m.GetUint("count_valid_replicating_slaves")
		a.CountSlavesFailingToConnectToMaster = m.GetUint("count_slaves_failing_to_connect_to_master")
		a.CountStaleSlaves = m.GetUint("count_stale_slaves")
		a.ReplicationDepth = m.GetUint("replication_depth")
		a.IsFailingToConnectToMaster = m.GetBool("is_failing_to_connect_to_master")
		a.IsDowntimed = m.GetBool("is_downtimed")
		a.DowntimeEndTimestamp = m.GetString("downtime_end_timestamp")
		a.DowntimeRemainingSeconds = m.GetInt("downtime_remaining_seconds")
		a.IsBinlogServer = m.GetBool("is_binlog_server")
		a.ClusterDetails.ReadRecoveryInfo()

		a.SlaveHosts = *NewInstanceKeyMap()
		a.SlaveHosts.ReadCommaDelimitedList(m.GetString("slave_hosts"))

		countValidOracleGTIDSlaves := m.GetUint("count_valid_oracle_gtid_slaves")
		a.OracleGTIDImmediateTopology = countValidOracleGTIDSlaves == a.CountValidSlaves && a.CountValidSlaves > 0
		countValidMariaDBGTIDSlaves := m.GetUint("count_valid_mariadb_gtid_slaves")
		a.MariaDBGTIDImmediateTopology = countValidMariaDBGTIDSlaves == a.CountValidSlaves && a.CountValidSlaves > 0
		countValidBinlogServerSlaves := m.GetUint("count_valid_binlog_server_slaves")
		a.BinlogServerImmediateTopology = countValidBinlogServerSlaves == a.CountValidSlaves && a.CountValidSlaves > 0
		a.PseudoGTIDImmediateTopology = m.GetBool("is_pseudo_gtid")

		a.CountStatementBasedLoggingSlaves = m.GetUint("count_statement_based_loggin_slaves")
		a.CountMixedBasedLoggingSlaves = m.GetUint("count_mixed_based_loggin_slaves")
		a.CountRowBasedLoggingSlaves = m.GetUint("count_row_based_loggin_slaves")
		a.CountDistinctMajorVersionsLoggingSlaves = m.GetUint("count_distinct_logging_major_versions")

		if a.IsMaster && !a.LastCheckValid && a.CountSlaves == 0 {
			a.Analysis = DeadMasterWithoutSlaves
			a.Description = "Master cannot be reached by bakufu and has no slave"
			//
		} else if a.IsMaster && !a.LastCheckValid && a.CountValidSlaves == a.CountSlaves && a.CountValidReplicatingSlaves == 0 {
			a.Analysis = DeadMaster
			a.Description = "Master cannot be reached by bakufu and none of its slaves is replicating"
			//
		} else if a.IsMaster && !a.LastCheckValid && a.CountSlaves > 0 && a.CountValidSlaves == 0 && a.CountValidReplicatingSlaves == 0 {
			a.Analysis = DeadMasterAndSlaves
			a.Description = "Master cannot be reached by bakufu and none of its slaves is replicating"
			//
		} else if a.IsMaster && !a.LastCheckValid && a.CountValidSlaves < a.CountSlaves && a.CountValidSlaves > 0 && a.CountValidReplicatingSlaves == 0 {
			a.Analysis = DeadMasterAndSomeSlaves
			a.Description = "Master cannot be reached by bakufu; some of its slaves are unreachable and none of its reachable slaves is replicating"
			//
		} else if a.IsMaster && !a.LastCheckValid && a.CountStaleSlaves == a.CountSlaves && a.CountValidReplicatingSlaves > 0 {
			a.Analysis = UnreachableMasterWithStaleSlaves
			a.Description = "Master cannot be reached by bakufu and has running yet stale slaves"
			//
		} else if a.IsMaster && !a.LastCheckValid && a.CountValidSlaves > 0 && a.CountValidReplicatingSlaves > 0 {
			a.Analysis = UnreachableMaster
			a.Description = "Master cannot be reached by bakufu but it has replicating slaves; possibly a network/host issue"
			//
		} else if a.IsMaster && a.LastCheckValid && a.CountSlaves == 1 && a.CountValidSlaves == a.CountSlaves && a.CountValidReplicatingSlaves == 0 {
			a.Analysis = MasterSingleSlaveNotReplicating
			a.Description = "Master is reachable but its single slave is not replicating"
			//
		} else if a.IsMaster && a.LastCheckValid && a.CountSlaves == 1 && a.CountValidSlaves == 0 {
			a.Analysis = MasterSingleSlaveDead
			a.Description = "Master is reachable but its single slave is dead"
			//
		} else if a.IsMaster && a.LastCheckValid && a.CountSlaves > 1 && a.CountValidSlaves == a.CountSlaves && a.CountValidReplicatingSlaves == 0 {
			a.Analysis = AllMasterSlavesNotReplicating
			a.Description = "Master is reachable but none of its slaves is replicating"
			//
		} else if a.IsMaster && a.LastCheckValid && a.CountSlaves > 1 && a.CountValidSlaves < a.CountSlaves && a.CountValidSlaves > 0 && a.CountValidReplicatingSlaves == 0 {
			a.Analysis = AllMasterSlavesNotReplicatingOrDead
			a.Description = "Master is reachable but none of its slaves is replicating"
			//
		} else if a.IsMaster && a.LastCheckValid && a.CountSlaves > 1 && a.CountStaleSlaves == a.CountSlaves && a.CountValidSlaves > 0 && a.CountValidReplicatingSlaves > 0 {
			a.Analysis = AllMasterSlavesStale
			a.Description = "Master is reachable but all of its slaves are stale, although attempting to replicate"
			//
		} else /* co-master */ if a.IsCoMaster && !a.LastCheckValid && a.CountSlaves > 0 && a.CountValidSlaves == a.CountSlaves && a.CountValidReplicatingSlaves == 0 {
			a.Analysis = DeadCoMaster
			a.Description = "Co-master cannot be reached by bakufu and none of its slaves is replicating"
			//
		} else if a.IsCoMaster && !a.LastCheckValid && a.CountSlaves > 0 && a.CountValidSlaves < a.CountSlaves && a.CountValidSlaves > 0 && a.CountValidReplicatingSlaves == 0 {
			a.Analysis = DeadCoMasterAndSomeSlaves
			a.Description = "Co-master cannot be reached by bakufu; some of its slaves are unreachable and none of its reachable slaves is replicating"
			//
		} else if a.IsCoMaster && !a.LastCheckValid && a.CountValidSlaves > 0 && a.CountValidReplicatingSlaves > 0 {
			a.Analysis = UnreachableCoMaster
			a.Description = "Co-master cannot be reached by bakufu but it has replicating slaves; possibly a network/host issue"
			//
		} else if a.IsCoMaster && a.LastCheckValid && a.CountSlaves > 0 && a.CountValidReplicatingSlaves == 0 {
			a.Analysis = AllCoMasterSlavesNotReplicating
			a.Description = "Co-master is reachable but none of its slaves is replicating"
			//
		} else /* intermediate-master */ if !a.IsMaster && !a.LastCheckValid && a.CountSlaves == 1 && a.CountValidSlaves == a.CountSlaves && a.CountSlavesFailingToConnectToMaster == a.CountSlaves && a.CountValidReplicatingSlaves == 0 {
			a.Analysis = DeadIntermediateMasterWithSingleSlaveFailingToConnect
			a.Description = "Intermediate master cannot be reached by bakufu and its (single) slave is failing to connect"
			//
		} else /* intermediate-master */ if !a.IsMaster && !a.LastCheckValid && a.CountSlaves == 1 && a.CountValidSlaves == a.CountSlaves && a.CountValidReplicatingSlaves == 0 {
			a.Analysis = DeadIntermediateMasterWithSingleSlave
			a.Description = "Intermediate master cannot be reached by bakufu and its (single) slave is not replicating"
			//
		} else /* intermediate-master */ if !a.IsMaster && !a.LastCheckValid && a.CountSlaves > 1 && a.CountValidSlaves == a.CountSlaves && a.CountValidReplicatingSlaves == 0 {
			a.Analysis = DeadIntermediateMaster
			a.Description = "Intermediate master cannot be reached by bakufu and none of its slaves is replicating"
			//
		} else if !a.IsMaster && !a.LastCheckValid && a.CountValidSlaves < a.CountSlaves && a.CountValidSlaves > 0 && a.CountValidReplicatingSlaves == 0 {
			a.Analysis = DeadIntermediateMasterAndSomeSlaves
			a.Description = "Intermediate master cannot be reached by bakufu; some of its slaves are unreachable and none of its reachable slaves is replicating"
			//
		} else if !a.IsMaster && !a.LastCheckValid && a.CountValidSlaves > 0 && a.CountValidReplicatingSlaves > 0 {
			a.Analysis = UnreachableIntermediateMaster
			a.Description = "Intermediate master cannot be reached by bakufu but it has replicating slaves; possibly a network/host issue"
			//
		} else if !a.IsMaster && a.LastCheckValid && a.CountSlaves > 1 && a.CountValidReplicatingSlaves == 0 &&
			a.CountSlavesFailingToConnectToMaster > 0 && a.CountSlavesFailingToConnectToMaster == a.CountValidSlaves {
			a.Analysis = AllIntermediateMasterSlavesFailingToConnectOrDead
			a.Description = "Intermediate master is reachable but all of its slaves are failing to connect"
			//
		} else if !a.IsMaster && a.LastCheckValid && a.CountSlaves > 0 && a.CountValidReplicatingSlaves == 0 {
			a.Analysis = AllIntermediateMasterSlavesNotReplicating
			a.Description = "Intermediate master is reachable but none of its slaves is replicating"
			//
		} else if a.IsBinlogServer && a.IsFailingToConnectToMaster {
			a.Analysis = BinlogServerFailingToConnectToMaster
			a.Description = "Binlog server is unable to connect to its master"
			//
		} else if a.ReplicationDepth == 1 && a.IsFailingToConnectToMaster {
			a.Analysis = FirstTierSlaveFailingToConnectToMaster
			a.Description = "1st tier slave (directly replicating from topology master) is unable to connect to the master"
			//
		}
		//		 else if a.IsMaster && a.CountSlaves == 0 {
		//			a.Analysis = MasterWithoutSlaves
		//			a.Description = "Master has no slaves"
		//		}

		appendAnalysis := func(analysis *ReplicationAnalysis) {
			if a.Analysis == NoProblem && len(a.StructureAnalysis) == 0 {
				return
			}
			skipThisHost := false
			for _, filter := range config.Config.RecoveryIgnoreHostnameFilters {
				if matched, _ := regexp.MatchString(filter, a.AnalyzedInstanceKey.Hostname); matched {
					skipThisHost = true
				}
			}
			if a.IsDowntimed && !includeDowntimed {
				skipThisHost = true
			}
			if !skipThisHost {
				result = append(result, a)
			}
		}

		{
			if a.CountStatementBasedLoggingSlaves > 0 && a.CountMixedBasedLoggingSlaves > 0 {
				a.StructureAnalysis = append(a.StructureAnalysis, StatementAndMixedLoggingSlavesStructureWarning)
			}
			if a.CountStatementBasedLoggingSlaves > 0 && a.CountRowBasedLoggingSlaves > 0 {
				a.StructureAnalysis = append(a.StructureAnalysis, StatementAndRowLoggingSlavesStructureWarning)
			}
			if a.CountMixedBasedLoggingSlaves > 0 && a.CountRowBasedLoggingSlaves > 0 {
				a.StructureAnalysis = append(a.StructureAnalysis, MixedAndRowLoggingSlavesStructureWarning)
			}
			if a.CountDistinctMajorVersionsLoggingSlaves > 1 {
				a.StructureAnalysis = append(a.StructureAnalysis, MultipleMajorVersionsLoggingSlaves)
			}
		}
		appendAnalysis(&a)

		if a.CountSlaves > 0 && auditAnalysis {
			go auditInstanceAnalysisInChangelog(&a.AnalyzedInstanceKey, a.Analysis)
		}
		return nil
	})

	if err != nil {
		log.Errore(err)
	}
	return result, err
}

func auditInstanceAnalysisInChangelog(instanceKey *InstanceKey, analysisCode AnalysisCode) error {
	if lastWrittenAnalysis, found := recentInstantAnalysis.Get(instanceKey.DisplayString()); found {
		if lastWrittenAnalysis == analysisCode {
			recentInstantAnalysis.Set(instanceKey.DisplayString(), analysisCode, cache.DefaultExpiration)
			return nil
		}
	}
	analysisChangeWriteAttemptCounter.Inc(1)
	sqlResult, err := db.ExecBakufu(`
			insert ignore into database_instance_last_analysis (
					hostname, port, analysis_timestamp, analysis
				) values (
					?, ?, now(), ?
				) on duplicate key update
					analysis = values(analysis),
					analysis_timestamp = if(analysis = values(analysis), analysis_timestamp, values(analysis_timestamp))
			`,
		instanceKey.Hostname, instanceKey.Port, string(analysisCode),
	)
	if err != nil {
		return log.Errore(err)
	}
	rows, err := sqlResult.RowsAffected()
	if err != nil {
		return log.Errore(err)
	}
	recentInstantAnalysis.Set(instanceKey.DisplayString(), analysisCode, cache.DefaultExpiration)
	lastAnalysisChanged := (rows > 0)

	if !lastAnalysisChanged {
		return nil
	}

	_, err = db.ExecBakufu(`
			insert into database_instance_analysis_changelog (
					hostname, port, analysis_timestamp, analysis
				) values (
					?, ?, now(), ?
				)
			`,
		instanceKey.Hostname, instanceKey.Port, string(analysisCode),
	)
	if err == nil {
		analysisChangeWriteCounter.Inc(1)
	}
	return log.Errore(err)
}

func ExpireInstanceAnalysisChangelog() error {
	_, err := db.ExecBakufu(`
			delete
				from database_instance_analysis_changelog
			where
				analysis_timestamp < now() - interval ? hour
			`,
		config.Config.UnseenInstanceForgetHours,
	)
	return log.Errore(err)
}

func ReadReplicationAnalysisChangelog() ([]ReplicationAnalysisChangelog, error) {
	res := []ReplicationAnalysisChangelog{}
	query := `
		select
            hostname,
            port,
			group_concat(analysis_timestamp,';',analysis order by changelog_id) as changelog
		from
			database_instance_analysis_changelog
		group by
			hostname, port
		`
	err := db.QueryBakufuRowsMap(query, func(m sqlutils.RowMap) error {
		analysisChangelog := ReplicationAnalysisChangelog{}

		analysisChangelog.AnalyzedInstanceKey.Hostname = m.GetString("hostname")
		analysisChangelog.AnalyzedInstanceKey.Port = m.GetInt("port")
		analysisChangelog.Changelog = m.GetString("changelog")

		res = append(res, analysisChangelog)
		return nil
	})

	if err != nil {
		log.Errore(err)
	}
	return res, err
}
