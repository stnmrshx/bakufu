/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package inst

import (
	"database/sql"
	"fmt"
	"github.com/stnmrshx/golib/log"
	"github.com/stnmrshx/golib/sqlutils"
	"github.com/stnmrshx/bakufu/go/config"
	"github.com/stnmrshx/bakufu/go/db"
	"time"
)

const topologyConcurrency = 128

var topologyConcurrencyChan = make(chan bool, topologyConcurrency)

type OperationGTIDHint string

const (
	GTIDHintDeny    OperationGTIDHint = "NoGTID"
	GTIDHintNeutral                   = "GTIDHintNeutral"
	GTIDHintForce                     = "GTIDHintForce"
)

const sqlThreadPollDuration = 400 * time.Millisecond

func ExecInstance(instanceKey *InstanceKey, query string, args ...interface{}) (sql.Result, error) {
	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return nil, err
	}
	res, err := sqlutils.Exec(db, query, args...)
	return res, err
}

func ExecInstanceNoPrepare(instanceKey *InstanceKey, query string, args ...interface{}) (sql.Result, error) {
	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return nil, err
	}
	res, err := sqlutils.ExecNoPrepare(db, query, args...)
	return res, err
}

func ExecuteOnTopology(f func()) {
	topologyConcurrencyChan <- true
	defer func() { recover(); <-topologyConcurrencyChan }()
	f()
}

func ScanInstanceRow(instanceKey *InstanceKey, query string, dest ...interface{}) error {
	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return err
	}
	err = db.QueryRow(query).Scan(dest...)
	return err
}

func EmptyCommitInstance(instanceKey *InstanceKey) error {
	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	return err
}

func RefreshTopologyInstance(instanceKey *InstanceKey) (*Instance, error) {
	_, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return nil, err
	}

	inst, found, err := ReadInstance(instanceKey)
	if err != nil || !found {
		return nil, err
	}

	return inst, nil
}

func RefreshTopologyInstances(instances [](*Instance)) {
	barrier := make(chan InstanceKey)
	for _, instance := range instances {
		instance := instance
		go func() {
			defer func() { barrier <- instance.Key }()
			ExecuteOnTopology(func() {
				log.Debugf("... reading instance: %+v", instance.Key)
				ReadTopologyInstance(&instance.Key)
			})
		}()
	}
	for range instances {
		<-barrier
	}
}

func RefreshInstanceSlaveHosts(instanceKey *InstanceKey) (*Instance, error) {
	_, _ = ExecInstance(instanceKey, `flush error logs`)
	_, _ = ExecInstance(instanceKey, `flush error logs`)

	instance, err := ReadTopologyInstance(instanceKey)
	return instance, err
}

func GetSlaveRestartPreserveStatements(instanceKey *InstanceKey, injectedStatement string) (statements []string, err error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return statements, err
	}
	if instance.Slave_IO_Running {
		statements = append(statements, SemicolonTerminated(`stop slave io_thread`))
	}
	if instance.Slave_SQL_Running {
		statements = append(statements, SemicolonTerminated(`stop slave sql_thread`))
	}
	if injectedStatement != "" {
		statements = append(statements, SemicolonTerminated(injectedStatement))
	}
	if instance.Slave_SQL_Running {
		statements = append(statements, SemicolonTerminated(`start slave sql_thread`))
	}
	if instance.Slave_IO_Running {
		statements = append(statements, SemicolonTerminated(`start slave io_thread`))
	}
	return statements, err
}

func FlushBinaryLogs(instanceKey *InstanceKey, count int) (*Instance, error) {
	if *config.RuntimeCLIFlags.Noop {
		return nil, fmt.Errorf("noop: aborting flush-binary-logs operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	for i := 0; i < count; i++ {
		_, err := ExecInstance(instanceKey, `flush binary logs`)
		if err != nil {
			return nil, log.Errore(err)
		}
	}

	log.Infof("flush-binary-logs count=%+v on %+v", count, *instanceKey)
	AuditOperation("flush-binary-logs", instanceKey, "success")

	return ReadTopologyInstance(instanceKey)
}

func FlushBinaryLogsTo(instanceKey *InstanceKey, logFile string) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	distance := instance.SelfBinlogCoordinates.FileNumberDistance(&BinlogCoordinates{LogFile: logFile})
	if distance < 0 {
		return nil, log.Errorf("FlushBinaryLogsTo: target log file %+v is smaller than current log file %+v", logFile, instance.SelfBinlogCoordinates.LogFile)
	}
	return FlushBinaryLogs(instanceKey, distance)
}

func PurgeBinaryLogsTo(instanceKey *InstanceKey, logFile string) (*Instance, error) {
	if *config.RuntimeCLIFlags.Noop {
		return nil, fmt.Errorf("noop: aborting purge-binary-logs operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	_, err := ExecInstanceNoPrepare(instanceKey, fmt.Sprintf("purge binary logs to '%s'", logFile))
	if err != nil {
		return nil, log.Errore(err)
	}

	log.Infof("purge-binary-logs to=%+v on %+v", logFile, *instanceKey)
	AuditOperation("purge-binary-logs", instanceKey, "success")

	return ReadTopologyInstance(instanceKey)
}

func PurgeBinaryLogsToCurrent(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}
	return PurgeBinaryLogsTo(instanceKey, instance.SelfBinlogCoordinates.LogFile)
}

func StopSlaveNicely(instanceKey *InstanceKey, timeout time.Duration) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if !instance.IsSlave() {
		return instance, fmt.Errorf("instance is not a slave: %+v", instanceKey)
	}

	_, err = ExecInstanceNoPrepare(instanceKey, `stop slave io_thread`)
	_, err = ExecInstanceNoPrepare(instanceKey, `start slave sql_thread`)

	if instance.SQLDelay == 0 {
		startTime := time.Now()
		for upToDate := false; !upToDate; {
			if timeout > 0 && time.Since(startTime) >= timeout {
				return nil, log.Errorf("StopSlaveNicely timeout on %+v", *instanceKey)
			}
			instance, err = ReadTopologyInstance(instanceKey)
			if err != nil {
				return instance, log.Errore(err)
			}

			if instance.SQLThreadUpToDate() {
				upToDate = true
			} else {
				time.Sleep(sqlThreadPollDuration)
			}
		}
	}
	_, err = ExecInstanceNoPrepare(instanceKey, `stop slave`)
	if err != nil {
		if instance.isMaxScale() && err.Error() == "Error 1199: Slave connection is not running" {
			err = nil
		}
	}
	if err != nil {
		return instance, log.Errore(err)
	}

	instance, err = ReadTopologyInstance(instanceKey)
	log.Infof("Stopped slave nicely on %+v, Self:%+v, Exec:%+v", *instanceKey, instance.SelfBinlogCoordinates, instance.ExecBinlogCoordinates)
	return instance, err
}

func StopSlavesNicely(slaves [](*Instance), timeout time.Duration) [](*Instance) {
	refreshedSlaves := [](*Instance){}

	log.Debugf("Stopping %d slaves nicely", len(slaves))
	barrier := make(chan *Instance)
	for _, slave := range slaves {
		slave := slave
		go func() {
			updatedSlave := &slave
			defer func() { barrier <- *updatedSlave }()
			ExecuteOnTopology(func() {
				StopSlaveNicely(&slave.Key, timeout)
				slave, _ = StopSlave(&slave.Key)
				updatedSlave = &slave
			})
		}()
	}
	for range slaves {
		refreshedSlaves = append(refreshedSlaves, <-barrier)
	}
	return refreshedSlaves
}

func StopSlave(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if !instance.IsSlave() {
		return instance, fmt.Errorf("instance is not a slave: %+v", instanceKey)
	}
	_, err = ExecInstanceNoPrepare(instanceKey, `stop slave`)
	if err != nil {
		if instance.isMaxScale() && err.Error() == "Error 1199: Slave connection is not running" {
			err = nil
		}
	}
	if err != nil {

		return instance, log.Errore(err)
	}
	instance, err = ReadTopologyInstance(instanceKey)

	log.Infof("Stopped slave on %+v, Self:%+v, Exec:%+v", *instanceKey, instance.SelfBinlogCoordinates, instance.ExecBinlogCoordinates)
	return instance, err
}

func StartSlave(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if !instance.IsSlave() {
		return instance, fmt.Errorf("instance is not a slave: %+v", instanceKey)
	}

	_, err = ExecInstanceNoPrepare(instanceKey, `start slave`)
	if err != nil {
		return instance, log.Errore(err)
	}
	log.Infof("Started slave on %+v", instanceKey)
	if config.Config.SlaveStartPostWaitMilliseconds > 0 {
		time.Sleep(time.Duration(config.Config.SlaveStartPostWaitMilliseconds) * time.Millisecond)
	}

	instance, err = ReadTopologyInstance(instanceKey)
	return instance, err
}

func RestartSlave(instanceKey *InstanceKey) (instance *Instance, err error) {
	instance, err = StopSlave(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}
	instance, err = StartSlave(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}
	return instance, nil

}

func StartSlaves(slaves [](*Instance)) {
	log.Debugf("Starting %d slaves", len(slaves))
	barrier := make(chan InstanceKey)
	for _, instance := range slaves {
		instance := instance
		go func() {
			defer func() { barrier <- instance.Key }()
			ExecuteOnTopology(func() { StartSlave(&instance.Key) })
		}()
	}
	for range slaves {
		<-barrier
	}
}

func StartSlaveUntilMasterCoordinates(instanceKey *InstanceKey, masterCoordinates *BinlogCoordinates) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if !instance.IsSlave() {
		return instance, fmt.Errorf("instance is not a slave: %+v", instanceKey)
	}
	if instance.SlaveRunning() {
		return instance, fmt.Errorf("slave already running: %+v", instanceKey)
	}

	log.Infof("Will start slave on %+v until coordinates: %+v", instanceKey, masterCoordinates)

	_, err = ExecInstanceNoPrepare(instanceKey, fmt.Sprintf("start slave until master_log_file='%s', master_log_pos=%d",
		masterCoordinates.LogFile, masterCoordinates.LogPos))
	if err != nil {
		return instance, log.Errore(err)
	}

	for upToDate := false; !upToDate; {
		instance, err = ReadTopologyInstance(instanceKey)
		if err != nil {
			return instance, log.Errore(err)
		}

		switch {
		case instance.ExecBinlogCoordinates.SmallerThan(masterCoordinates):
			time.Sleep(sqlThreadPollDuration)
		case instance.ExecBinlogCoordinates.Equals(masterCoordinates):
			upToDate = true
		case masterCoordinates.SmallerThan(&instance.ExecBinlogCoordinates):
			return instance, fmt.Errorf("Start SLAVE UNTIL is past coordinates: %+v", instanceKey)
		}
	}

	instance, err = StopSlave(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	return instance, err
}

func ChangeMasterCredentials(instanceKey *InstanceKey, masterUser string, masterPassword string) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}
	if masterUser == "" {
		return instance, log.Errorf("Empty user in ChangeMasterCredentials() for %+v", *instanceKey)
	}

	if instance.SlaveRunning() {
		return instance, fmt.Errorf("ChangeMasterTo: Cannot change master on: %+v because slave is running", *instanceKey)
	}
	log.Debugf("ChangeMasterTo: will attempt changing master credentials on %+v", *instanceKey)

	if *config.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting CHANGE MASTER TO operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}
	_, err = ExecInstanceNoPrepare(instanceKey, fmt.Sprintf("change master to master_user='%s', master_password='%s'",
		masterUser, masterPassword))

	if err != nil {
		return instance, log.Errore(err)
	}

	log.Infof("ChangeMasterTo: Changed master credentials on %+v", *instanceKey)

	instance, err = ReadTopologyInstance(instanceKey)
	return instance, err
}

func ChangeMasterTo(instanceKey *InstanceKey, masterKey *InstanceKey, masterBinlogCoordinates *BinlogCoordinates, skipUnresolve bool, gtidHint OperationGTIDHint) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if instance.SlaveRunning() {
		return instance, fmt.Errorf("ChangeMasterTo: Cannot change master on: %+v because slave is running", *instanceKey)
	}
	log.Debugf("ChangeMasterTo: will attempt changing master on %+v to %+v, %+v", *instanceKey, *masterKey, *masterBinlogCoordinates)
	changeToMasterKey := masterKey
	if !skipUnresolve {
		unresolvedMasterKey, nameUnresolved, err := UnresolveHostname(masterKey)
		if err != nil {
			log.Debugf("ChangeMasterTo: aborting operation on %+v due to resolving error on %+v: %+v", *instanceKey, *masterKey, err)
			return instance, err
		}
		if nameUnresolved {
			log.Debugf("ChangeMasterTo: Unresolved %+v into %+v", *masterKey, unresolvedMasterKey)
		}
		changeToMasterKey = &unresolvedMasterKey
	}

	if *config.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting CHANGE MASTER TO operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	originalMasterKey := instance.MasterKey
	originalExecBinlogCoordinates := instance.ExecBinlogCoordinates

	changedViaGTID := false
	if instance.UsingMariaDBGTID && gtidHint != GTIDHintDeny {
		_, err = ExecInstanceNoPrepare(instanceKey, fmt.Sprintf("change master to master_host='%s', master_port=%d",
			changeToMasterKey.Hostname, changeToMasterKey.Port))
		changedViaGTID = true
	} else if instance.UsingMariaDBGTID && gtidHint == GTIDHintDeny {
		_, err = ExecInstanceNoPrepare(instanceKey, fmt.Sprintf("change master to master_host='%s', master_port=%d, master_log_file='%s', master_log_pos=%d, master_use_gtid=no",
			changeToMasterKey.Hostname, changeToMasterKey.Port, masterBinlogCoordinates.LogFile, masterBinlogCoordinates.LogPos))
	} else if instance.IsMariaDB() && gtidHint == GTIDHintForce {
		_, err = ExecInstanceNoPrepare(instanceKey, fmt.Sprintf("change master to master_host='%s', master_port=%d, master_use_gtid=slave_pos",
			changeToMasterKey.Hostname, changeToMasterKey.Port))
		changedViaGTID = true
	} else if instance.UsingOracleGTID && gtidHint != GTIDHintDeny {
		_, err = ExecInstanceNoPrepare(instanceKey, fmt.Sprintf("change master to master_host='%s', master_port=%d",
			changeToMasterKey.Hostname, changeToMasterKey.Port))
		changedViaGTID = true
	} else if instance.UsingOracleGTID && gtidHint == GTIDHintDeny {
		_, err = ExecInstanceNoPrepare(instanceKey, fmt.Sprintf("change master to master_host='%s', master_port=%d, master_log_file='%s', master_log_pos=%d, master_auto_position=0",
			changeToMasterKey.Hostname, changeToMasterKey.Port, masterBinlogCoordinates.LogFile, masterBinlogCoordinates.LogPos))
	} else if instance.SupportsOracleGTID && gtidHint == GTIDHintForce {
		_, err = ExecInstanceNoPrepare(instanceKey, fmt.Sprintf("change master to master_host='%s', master_port=%d, master_auto_position=1",
			changeToMasterKey.Hostname, changeToMasterKey.Port))
		changedViaGTID = true
	} else {
		_, err = ExecInstanceNoPrepare(instanceKey, fmt.Sprintf("change master to master_host='%s', master_port=%d, master_log_file='%s', master_log_pos=%d",
			changeToMasterKey.Hostname, changeToMasterKey.Port, masterBinlogCoordinates.LogFile, masterBinlogCoordinates.LogPos))
	}
	if err != nil {
		return instance, log.Errore(err)
	}
	WriteMasterPositionEquivalence(&originalMasterKey, &originalExecBinlogCoordinates, changeToMasterKey, masterBinlogCoordinates)

	log.Infof("ChangeMasterTo: Changed master on %+v to: %+v, %+v. GTID: %+v", *instanceKey, masterKey, masterBinlogCoordinates, changedViaGTID)

	instance, err = ReadTopologyInstance(instanceKey)
	return instance, err
}

func SkipToNextBinaryLog(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	nextFileCoordinates, err := instance.ExecBinlogCoordinates.NextFileCoordinates()
	if err != nil {
		return instance, log.Errore(err)
	}
	nextFileCoordinates.LogPos = 4
	log.Debugf("Will skip replication on %+v to next binary log: %+v", instance.Key, nextFileCoordinates.LogFile)

	instance, err = ChangeMasterTo(&instance.Key, &instance.MasterKey, &nextFileCoordinates, false, GTIDHintNeutral)
	if err != nil {
		return instance, log.Errore(err)
	}
	AuditOperation("skip-binlog", instanceKey, fmt.Sprintf("Skipped replication to next binary log: %+v", nextFileCoordinates.LogFile))
	return StartSlave(instanceKey)
}

func ResetSlave(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if instance.SlaveRunning() {
		return instance, fmt.Errorf("Cannot reset slave on: %+v because slave is running", instanceKey)
	}

	if *config.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting reset-slave operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	_, err = ExecInstanceNoPrepare(instanceKey, `change master to master_host='_'`)
	if err != nil {
		return instance, log.Errore(err)
	}
	_, err = ExecInstanceNoPrepare(instanceKey, `reset slave /*!50603 all */`)
	if err != nil {
		return instance, log.Errore(err)
	}
	log.Infof("Reset slave %+v", instanceKey)

	instance, err = ReadTopologyInstance(instanceKey)
	return instance, err
}

func ResetMaster(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if instance.SlaveRunning() {
		return instance, fmt.Errorf("Cannot reset master on: %+v because slave is running", instanceKey)
	}

	if *config.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting reset-master operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	_, err = ExecInstanceNoPrepare(instanceKey, `reset master`)
	if err != nil {
		return instance, log.Errore(err)
	}
	log.Infof("Reset master %+v", instanceKey)

	instance, err = ReadTopologyInstance(instanceKey)
	return instance, err
}

func setGTIDPurged(instance *Instance, gtidPurged string) error {
	if *config.RuntimeCLIFlags.Noop {
		return fmt.Errorf("noop: aborting set-gtid-purged operation on %+v; signalling error but nothing went wrong.", instance.Key)
	}

	_, err := ExecInstance(&instance.Key, fmt.Sprintf(`set global gtid_purged := '%s'`, gtidPurged))
	return err
}

func skipQueryClassic(instance *Instance) error {
	_, err := ExecInstance(&instance.Key, `set global sql_slave_skip_counter := 1`)
	return err
}

func skipQueryOracleGtid(instance *Instance) error {
	nextGtid, err := instance.NextGTID()
	if err != nil {
		return err
	}
	if nextGtid == "" {
		return fmt.Errorf("Empty NextGTID() in skipQueryGtid() for %+v", instance.Key)
	}
	if _, err := ExecInstanceNoPrepare(&instance.Key, fmt.Sprintf(`SET GTID_NEXT='%s'`, nextGtid)); err != nil {
		return err
	}
	if err := EmptyCommitInstance(&instance.Key); err != nil {
		return err
	}
	if _, err := ExecInstanceNoPrepare(&instance.Key, `SET GTID_NEXT='AUTOMATIC'`); err != nil {
		return err
	}
	return nil
}

func SkipQuery(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if !instance.IsSlave() {
		return instance, fmt.Errorf("instance is not a slave: %+v", instanceKey)
	}
	if instance.Slave_SQL_Running {
		return instance, fmt.Errorf("Slave SQL thread is running on %+v", instanceKey)
	}
	if instance.LastSQLError == "" {
		return instance, fmt.Errorf("No SQL error on %+v", instanceKey)
	}

	if *config.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting skip-query operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	log.Debugf("Skipping one query on %+v", instanceKey)
	if instance.UsingOracleGTID {
		err = skipQueryOracleGtid(instance)
	} else if instance.UsingMariaDBGTID {
		return instance, log.Errorf("%+v is replicating with MariaDB GTID. To skip a query first disable GTID, then skip, then enable GTID again", *instanceKey)
	} else {
		err = skipQueryClassic(instance)
	}
	if err != nil {
		return instance, log.Errore(err)
	}
	AuditOperation("skip-query", instanceKey, "Skipped one query")
	return StartSlave(instanceKey)
}

func DetachSlave(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if instance.SlaveRunning() {
		return instance, fmt.Errorf("Cannot detach slave on: %+v because slave is running", instanceKey)
	}

	isDetached, _, _ := instance.ExecBinlogCoordinates.DetachedCoordinates()

	if isDetached {
		return instance, fmt.Errorf("Cannot (need not) detach slave on: %+v because slave is already detached", instanceKey)
	}

	if *config.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting detach-slave operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	detachedCoordinates := BinlogCoordinates{LogFile: fmt.Sprintf("//%s:%d", instance.ExecBinlogCoordinates.LogFile, instance.ExecBinlogCoordinates.LogPos), LogPos: instance.ExecBinlogCoordinates.LogPos}
	_, err = ExecInstanceNoPrepare(instanceKey, fmt.Sprintf(`change master to master_log_file='%s', master_log_pos=%d`, detachedCoordinates.LogFile, detachedCoordinates.LogPos))
	if err != nil {
		return instance, log.Errore(err)
	}

	log.Infof("Detach slave %+v", instanceKey)

	instance, err = ReadTopologyInstance(instanceKey)
	return instance, err
}

func ReattachSlave(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if instance.SlaveRunning() {
		return instance, fmt.Errorf("Cannot (need not) reattach slave on: %+v because slave is running", instanceKey)
	}

	isDetached, detachedLogFile, detachedLogPos := instance.ExecBinlogCoordinates.DetachedCoordinates()

	if !isDetached {
		return instance, fmt.Errorf("Cannot reattach slave on: %+v because slave is not detached", instanceKey)
	}

	if *config.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting reattach-slave operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	_, err = ExecInstanceNoPrepare(instanceKey, fmt.Sprintf(`change master to master_log_file='%s', master_log_pos=%s`, detachedLogFile, detachedLogPos))
	if err != nil {
		return instance, log.Errore(err)
	}

	log.Infof("Reattach slave %+v", instanceKey)

	instance, err = ReadTopologyInstance(instanceKey)
	return instance, err
}

func MasterPosWait(instanceKey *InstanceKey, binlogCoordinates *BinlogCoordinates) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	_, err = ExecInstance(instanceKey, `select master_pos_wait(?, ?)`, binlogCoordinates.LogFile, binlogCoordinates.LogPos)
	if err != nil {
		return instance, log.Errore(err)
	}
	log.Infof("Instance %+v has reached coordinates: %+v", instanceKey, binlogCoordinates)

	instance, err = ReadTopologyInstance(instanceKey)
	return instance, err
}

func ReadReplicationCredentials(instanceKey *InstanceKey) (replicationUser string, replicationPassword string, err error) {
	query := `
		select
			ifnull(max(User_name), '') as user,
			ifnull(max(User_password), '') as password
		from
			mysql.slave_master_info
	`
	err = ScanInstanceRow(instanceKey, query, &replicationUser, &replicationPassword)
	if err != nil {
		return replicationUser, replicationPassword, err
	}
	if replicationUser == "" {
		err = fmt.Errorf("Cannot find credentials in mysql.slave_master_info")
	}
	return replicationUser, replicationPassword, err
}

func SetReadOnly(instanceKey *InstanceKey, readOnly bool) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if *config.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting set-read-only operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	_, err = ExecInstance(instanceKey, fmt.Sprintf("set global read_only = %t", readOnly))
	if err != nil {
		return instance, log.Errore(err)
	}
	instance, err = ReadTopologyInstance(instanceKey)

	log.Infof("instance %+v read_only: %t", instanceKey, readOnly)
	AuditOperation("read-only", instanceKey, fmt.Sprintf("set as %t", readOnly))

	return instance, err
}

func KillQuery(instanceKey *InstanceKey, process int64) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if *config.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting kill-query operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	_, err = ExecInstance(instanceKey, fmt.Sprintf(`kill query %d`, process))
	if err != nil {
		return instance, log.Errore(err)
	}

	instance, err = ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	log.Infof("Killed query on %+v", *instanceKey)
	AuditOperation("kill-query", instanceKey, fmt.Sprintf("Killed query %d", process))
	return instance, err
}
