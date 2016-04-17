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
	"github.com/stnmrshx/golib/math"
	"github.com/stnmrshx/bakufu/go/config"
	"strconv"
	"strings"
)

type CandidatePromotionRule string

const (
	MustPromoteRule      CandidatePromotionRule = "must"
	PreferPromoteRule                           = "prefer"
	NeutralPromoteRule                          = "neutral"
	PreferNotPromoteRule                        = "prefer_not"
	MustNotPromoteRule                          = "must_not"
)

type Instance struct {
	Key                    InstanceKey
	Uptime                 uint
	ServerID               uint
	ServerUUID             string
	Version                string
	ReadOnly               bool
	Binlog_format          string
	LogBinEnabled          bool
	LogSlaveUpdatesEnabled bool
	SelfBinlogCoordinates  BinlogCoordinates
	MasterKey              InstanceKey
	Slave_SQL_Running      bool
	Slave_IO_Running       bool
	HasReplicationFilters  bool
	SupportsOracleGTID     bool
	UsingOracleGTID        bool
	UsingMariaDBGTID       bool
	UsingPseudoGTID        bool
	ReadBinlogCoordinates  BinlogCoordinates
	ExecBinlogCoordinates  BinlogCoordinates
	IsDetached             bool
	RelaylogCoordinates    BinlogCoordinates
	LastSQLError           string
	LastIOError            string
	SecondsBehindMaster    sql.NullInt64
	SQLDelay               uint
	ExecutedGtidSet        string
	GtidPurged             string

	SlaveLagSeconds                 sql.NullInt64
	SlaveHosts                      InstanceKeyMap
	ClusterName                     string
	SuggestedClusterAlias           string
	DataCenter                      string
	PhysicalEnvironment             string
	ReplicationDepth                uint
	IsCoMaster                      bool
	HasReplicationCredentials       bool
	ReplicationCredentialsAvailable bool

	LastSeenTimestamp    string
	IsLastCheckValid     bool
	IsUpToDate           bool
	IsRecentlyChecked    bool
	SecondsSinceLastSeen sql.NullInt64
	CountMySQLSnapshots  int

	IsCandidate          bool
	PromotionRule        CandidatePromotionRule
	IsDowntimed          bool
	DowntimeReason       string
	DowntimeOwner        string
	DowntimeEndTimestamp string
	UnresolvedHostname   string
}

func NewInstance() *Instance {
	return &Instance{
		SlaveHosts: make(map[InstanceKey]bool),
	}
}

func (this *Instance) Equals(other *Instance) bool {
	return this.Key == other.Key
}

func (this *Instance) MajorVersion() []string {
	return strings.Split(this.Version, ".")[:2]
}

func (this *Instance) IsMySQL51() bool {
	return strings.Join(this.MajorVersion(), ".") == "5.1"
}

func (this *Instance) IsMySQL55() bool {
	return strings.Join(this.MajorVersion(), ".") == "5.5"
}

func (this *Instance) IsMySQL56() bool {
	return strings.Join(this.MajorVersion(), ".") == "5.6"
}

func (this *Instance) IsMySQL57() bool {
	return strings.Join(this.MajorVersion(), ".") == "5.7"
}

func (this *Instance) IsMySQL58() bool {
	return strings.Join(this.MajorVersion(), ".") == "5.8"
}

func (this *Instance) IsSmallerMajorVersion(other *Instance) bool {
	thisMajorVersion := this.MajorVersion()
	otherMajorVersion := other.MajorVersion()
	for i := 0; i < len(thisMajorVersion); i++ {
		this_token, _ := strconv.Atoi(thisMajorVersion[i])
		other_token, _ := strconv.Atoi(otherMajorVersion[i])
		if this_token < other_token {
			return true
		}
		if this_token > other_token {
			return false
		}
	}
	return false
}

func (this *Instance) IsSmallerMajorVersionByString(otherVersion string) bool {
	other := &Instance{Version: otherVersion}
	return this.IsSmallerMajorVersion(other)
}

func (this *Instance) IsMariaDB() bool {
	return strings.Contains(this.Version, "MariaDB")
}

func (this *Instance) isMaxScale() bool {
	return strings.Contains(this.Version, "maxscale")
}

func (this *Instance) IsBinlogServer() bool {
	if this.isMaxScale() {
		return true
	}
	return false
}

func (this *Instance) IsOracleMySQL() bool {
	if this.IsMariaDB() {
		return false
	}
	if this.isMaxScale() {
		return false
	}
	if this.IsBinlogServer() {
		return false
	}
	return true
}

func (this *Instance) IsSlave() bool {
	return this.MasterKey.Hostname != "" && this.MasterKey.Hostname != "_" && this.MasterKey.Port != 0 && (this.ReadBinlogCoordinates.LogFile != "" || this.UsingGTID())
}

func (this *Instance) SlaveRunning() bool {
	return this.IsSlave() && this.Slave_SQL_Running && this.Slave_IO_Running
}

func (this *Instance) SQLThreadUpToDate() bool {
	return this.ReadBinlogCoordinates.Equals(&this.ExecBinlogCoordinates)
}

func (this *Instance) UsingGTID() bool {
	return this.UsingOracleGTID || this.UsingMariaDBGTID
}

func (this *Instance) NextGTID() (string, error) {
	if this.ExecutedGtidSet == "" {
		return "", fmt.Errorf("No value found in Executed_Gtid_Set; cannot compute NextGTID")
	}

	firstToken := func(s string, delimiter string) string {
		tokens := strings.Split(s, delimiter)
		return tokens[0]
	}
	lastToken := func(s string, delimiter string) string {
		tokens := strings.Split(s, delimiter)
		return tokens[len(tokens)-1]
	}
	executedGTIDsFromMaster := lastToken(this.ExecutedGtidSet, ",")
	executedRange := lastToken(executedGTIDsFromMaster, ":")
	lastExecutedNumberToken := lastToken(executedRange, "-")
	lastExecutedNumber, err := strconv.Atoi(lastExecutedNumberToken)
	if err != nil {
		return "", err
	}
	nextNumber := lastExecutedNumber + 1
	nextGTID := fmt.Sprintf("%s:%d", firstToken(executedGTIDsFromMaster, ":"), nextNumber)
	return nextGTID, nil
}

func (this *Instance) AddSlaveKey(slaveKey *InstanceKey) {
	this.SlaveHosts.AddKey(*slaveKey)
}

func (this *Instance) GetNextBinaryLog(binlogCoordinates BinlogCoordinates) (BinlogCoordinates, error) {
	if binlogCoordinates.LogFile == this.SelfBinlogCoordinates.LogFile {
		return binlogCoordinates, fmt.Errorf("Cannot find next binary log for %+v", binlogCoordinates)
	}
	return binlogCoordinates.NextFileCoordinates()
}

func (this *Instance) IsSlaveOf(master *Instance) bool {
	return this.MasterKey.Equals(&master.Key)
}

func (this *Instance) IsMasterOf(slave *Instance) bool {
	return slave.IsSlaveOf(this)
}

func (this *Instance) CanReplicateFrom(other *Instance) (bool, error) {
	if this.Key.Equals(&other.Key) {
		return false, fmt.Errorf("instance cannot replicate from itself: %+v", this.Key)
	}
	if !other.LogBinEnabled {
		return false, fmt.Errorf("instance does not have binary logs enabled: %+v", other.Key)
	}
	if other.IsSlave() {
		if !other.LogSlaveUpdatesEnabled {
			return false, fmt.Errorf("instance does not have log_slave_updates enabled: %+v", other.Key)
		}
	}
	if this.IsSmallerMajorVersion(other) && !this.IsBinlogServer() {
		return false, fmt.Errorf("instance %+v has version %s, which is lower than %s on %+v ", this.Key, this.Version, other.Version, other.Key)
	}
	if this.LogBinEnabled && this.LogSlaveUpdatesEnabled {
		if this.Binlog_format == "STATEMENT" && (other.Binlog_format == "ROW" || other.Binlog_format == "MIXED") {
			return false, fmt.Errorf("Cannot replicate from ROW/MIXED binlog format on %+v to STATEMENT on %+v", other.Key, this.Key)
		}
		if this.Binlog_format == "MIXED" && other.Binlog_format == "ROW" {
			return false, fmt.Errorf("Cannot replicate from ROW binlog format on %+v to MIXED on %+v", other.Key, this.Key)
		}
	}
	if config.Config.VerifyReplicationFilters {
		if other.HasReplicationFilters && !this.HasReplicationFilters {
			return false, fmt.Errorf("%+v has replication filters", other.Key)
		}
	}
	if this.ServerID == other.ServerID && !this.IsBinlogServer() {
		return false, fmt.Errorf("Identical server id: %+v, %+v both have %d", other.Key, this.Key, this.ServerID)
	}
	return true, nil
}

func (this *Instance) HasReasonableMaintenanceReplicationLag() bool {
	if this.SQLDelay > 0 {
		return math.AbsInt64(this.SecondsBehindMaster.Int64-int64(this.SQLDelay)) <= int64(config.Config.ReasonableMaintenanceReplicationLagSeconds)
	}
	return this.SecondsBehindMaster.Int64 <= int64(config.Config.ReasonableMaintenanceReplicationLagSeconds)
}

func (this *Instance) CanMove() (bool, error) {
	if !this.IsLastCheckValid {
		return false, fmt.Errorf("%+v: last check invalid", this.Key)
	}
	if !this.IsRecentlyChecked {
		return false, fmt.Errorf("%+v: not recently checked", this.Key)
	}
	if !this.Slave_SQL_Running {
		return false, fmt.Errorf("%+v: instance is not replicating", this.Key)
	}
	if !this.Slave_IO_Running {
		return false, fmt.Errorf("%+v: instance is not replicating", this.Key)
	}
	if !this.SecondsBehindMaster.Valid {
		return false, fmt.Errorf("%+v: cannot determine slave lag", this.Key)
	}
	if !this.HasReasonableMaintenanceReplicationLag() {
		return false, fmt.Errorf("%+v: lags too much", this.Key)
	}
	return true, nil
}

func (this *Instance) CanMoveAsCoMaster() (bool, error) {
	if !this.IsLastCheckValid {
		return false, fmt.Errorf("%+v: last check invalid", this.Key)
	}
	if !this.IsRecentlyChecked {
		return false, fmt.Errorf("%+v: not recently checked", this.Key)
	}
	return true, nil
}

func (this *Instance) CanMoveViaMatch() (bool, error) {
	if !this.IsLastCheckValid {
		return false, fmt.Errorf("%+v: last check invalid", this.Key)
	}
	if !this.IsRecentlyChecked {
		return false, fmt.Errorf("%+v: not recently checked", this.Key)
	}
	return true, nil
}

func (this *Instance) StatusString() string {
	if !this.IsLastCheckValid {
		return "invalid"
	}
	if !this.IsRecentlyChecked {
		return "unchecked"
	}
	if this.IsSlave() && !(this.Slave_SQL_Running && this.Slave_IO_Running) {
		return "nonreplicating"
	}
	if this.IsSlave() && this.SecondsBehindMaster.Int64 > int64(config.Config.ReasonableMaintenanceReplicationLagSeconds) {
		return "lag"
	}
	return "ok"
}

func (this *Instance) LagStatusString() string {
	if !this.IsLastCheckValid {
		return "unknown"
	}
	if !this.IsRecentlyChecked {
		return "unknown"
	}
	if this.IsSlave() && !(this.Slave_SQL_Running && this.Slave_IO_Running) {
		return "null"
	}
	if this.IsSlave() && !this.SecondsBehindMaster.Valid {
		return "null"
	}
	if this.IsSlave() && this.SecondsBehindMaster.Int64 > int64(config.Config.ReasonableMaintenanceReplicationLagSeconds) {
		return fmt.Sprintf("%+vs", this.SecondsBehindMaster.Int64)
	}
	return fmt.Sprintf("%+vs", this.SecondsBehindMaster.Int64)
}

func (this *Instance) HumanReadableDescription() string {
	tokens := []string{}
	tokens = append(tokens, this.LagStatusString())
	tokens = append(tokens, this.StatusString())
	tokens = append(tokens, this.Version)
	if this.ReadOnly {
		tokens = append(tokens, "ro")
	} else {
		tokens = append(tokens, "rw")
	}
	if this.LogBinEnabled {
		tokens = append(tokens, this.Binlog_format)
	} else {
		tokens = append(tokens, "nobinlog")
	}
	if this.LogBinEnabled && this.LogSlaveUpdatesEnabled {
		tokens = append(tokens, ">>")
	}
	if this.UsingGTID() {
		tokens = append(tokens, "GTID")
	}
	if this.UsingPseudoGTID {
		tokens = append(tokens, "P-GTID")
	}
	description := fmt.Sprintf("[%s]", strings.Join(tokens, ","))
	return description
}
