/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package config

import (
	"encoding/json"
	"os"
	"regexp"

	"gopkg.in/gcfg.v1"

	"github.com/stnmrshx/golib/log"
)

var (
	envVariableRegexp = regexp.MustCompile("[$][{](.*)[}]")
)

type Configuration struct {
	Debug                                        bool
	EnableSyslog                                 bool
	ListenAddress                                string
	AgentsServerPort                             string
	MySQLTopologyUser                            string
	MySQLTopologyPassword                        string
	MySQLTopologyCredentialsConfigFile           string
	MySQLTopologySSLPrivateKeyFile               string
	MySQLTopologySSLCertFile                     string
	MySQLTopologySSLCAFile                       string
	MySQLTopologySSLSkipVerify                   bool
	MySQLTopologyUseMutualTLS                    bool
	MySQLTopologyMaxPoolConnections              int
	DatabaselessMode__experimental               bool
	MySQLBakufuHost                        string
	MySQLBakufuPort                        uint
	MySQLBakufuDatabase                    string
	MySQLBakufuUser                        string
	MySQLBakufuPassword                    string
	MySQLBakufuCredentialsConfigFile       string
	MySQLBakufuSSLPrivateKeyFile           string
	MySQLBakufuSSLCertFile                 string
	MySQLBakufuSSLCAFile                   string
	MySQLBakufuSSLSkipVerify               bool
	MySQLBakufuUseMutualTLS                bool
	MySQLConnectTimeoutSeconds                   int
	DefaultInstancePort                          int
	SkipBakufuDatabaseUpdate               bool
	SlaveLagQuery                                string
	SlaveStartPostWaitMilliseconds               int
	DiscoverByShowSlaveHosts                     bool
	InstancePollSeconds                          uint
	ReadLongRunningQueries                       bool
	BinlogFileHistoryDays                        int
	UnseenInstanceForgetHours                    uint
	SnapshotTopologiesIntervalHours              uint
	DiscoveryPollSeconds                         uint
	InstanceBulkOperationsWaitTimeoutSeconds     uint
	ActiveNodeExpireSeconds                      uint
	HostnameResolveMethod                        string
	MySQLHostnameResolveMethod                   string
	SkipBinlogServerUnresolveCheck               bool
	ExpiryHostnameResolvesMinutes                int
	RejectHostnameResolvePattern                 string
	ReasonableReplicationLagSeconds              int
	ProblemIgnoreHostnameFilters                 []string
	VerifyReplicationFilters                     bool
	MaintenanceOwner                             string
	ReasonableMaintenanceReplicationLagSeconds   int
	MaintenanceExpireMinutes                     uint
	MaintenancePurgeDays                         uint
	CandidateInstanceExpireMinutes               uint
	AuditLogFile                                 string
	AuditToSyslog                                bool
	AuditPageSize                                int
	AuditPurgeDays                               uint
	RemoveTextFromHostnameDisplay                string
	ReadOnly                                     bool
	AuthenticationMethod                         string
	HTTPAuthUser                                 string
	HTTPAuthPassword                             string
	AuthUserHeader                               string
	PowerAuthUsers                               []string
	AccessTokenUseExpirySeconds                  uint
	AccessTokenExpiryMinutes                     uint
	ClusterNameToAlias                           map[string]string
	DetectClusterAliasQuery                      string
	DetectClusterDomainQuery                     string
	DataCenterPattern                            string
	PhysicalEnvironmentPattern                   string
	DetectDataCenterQuery                        string
	DetectPhysicalEnvironmentQuery               string
	SupportFuzzyPoolHostnames                    bool
	PromotionIgnoreHostnameFilters               []string
	ServeAgentsHttp                              bool
	AgentsUseSSL                                 bool
	AgentsUseMutualTLS                           bool
	AgentSSLSkipVerify                           bool
	AgentSSLPrivateKeyFile                       string
	AgentSSLCertFile                             string
	AgentSSLCAFile                               string
	AgentSSLValidOUs                             []string
	UseSSL                                       bool
	UseMutualTLS                                 bool
	SSLSkipVerify                                bool
	SSLPrivateKeyFile                            string
	SSLCertFile                                  string
	SSLCAFile                                    string
	SSLValidOUs                                  []string
	StatusEndpoint                               string
	StatusSimpleHealth                           bool
	StatusOUVerify                               bool
	HttpTimeoutSeconds                           int
	AgentPollMinutes                             uint
	AgentAutoDiscover                            bool
	UnseenAgentForgetHours                       uint
	StaleSeedFailMinutes                         uint
	SeedAcceptableBytesDiff                      int64
	PseudoGTIDPattern                            string
	PseudoGTIDPatternIsFixedSubstring            bool
	PseudoGTIDMonotonicHint                      string
	DetectPseudoGTIDQuery                        string
	PseudoGTIDCoordinatesHistoryHeuristicMinutes int
	BinlogEventsChunkSize                        int
	BufferBinlogEvents                           bool
	SkipBinlogEventsContaining                   []string
	ReduceReplicationAnalysisCount               bool
	FailureDetectionPeriodBlockMinutes           int
	RecoveryPollSeconds                          int
	RecoveryPeriodBlockMinutes                   int
	RecoveryPeriodBlockSeconds                   int
	RecoveryIgnoreHostnameFilters                []string
	RecoverMasterClusterFilters                  []string
	RecoverIntermediateMasterClusterFilters      []string
	OnFailureDetectionProcesses                  []string
	PreFailoverProcesses                         []string
	PostFailoverProcesses                        []string
	PostUnsuccessfulFailoverProcesses            []string
	PostMasterFailoverProcesses                  []string
	PostIntermediateMasterFailoverProcesses      []string
	CoMasterRecoveryMustPromoteOtherCoMaster     bool
	DetachLostSlavesAfterMasterFailover          bool
	ApplyMySQLPromotionAfterMasterFailover       bool
	MasterFailoverLostInstancesDowntimeMinutes   uint
	PostponeSlaveRecoveryOnLagMinutes            uint
	OSCIgnoreHostnameFilters                     []string
	GraphiteAddr                                 string
	GraphitePath                                 string
	GraphiteConvertHostnameDotsToUnderscores     bool
	GraphitePollSeconds                          int
}

func (this *Configuration) ToJSONString() string {
	b, _ := json.Marshal(this)
	return string(b)
}

var Config = newConfiguration()
var readFileNames []string

func newConfiguration() *Configuration {
	return &Configuration{
		Debug:                                        false,
		EnableSyslog:                                 false,
		ListenAddress:                                ":3000",
		AgentsServerPort:                             ":3001",
		StatusEndpoint:                               "/api/status",
		StatusSimpleHealth:                           true,
		StatusOUVerify:                               false,
		MySQLBakufuPort:                        3306,
		MySQLTopologyMaxPoolConnections:              3,
		MySQLTopologyUseMutualTLS:                    false,
		DatabaselessMode__experimental:               false,
		MySQLBakufuUseMutualTLS:                false,
		MySQLConnectTimeoutSeconds:                   5,
		DefaultInstancePort:                          3306,
		SkipBakufuDatabaseUpdate:               false,
		InstancePollSeconds:                          60,
		ReadLongRunningQueries:                       true,
		BinlogFileHistoryDays:                        0,
		UnseenInstanceForgetHours:                    240,
		SnapshotTopologiesIntervalHours:              0,
		SlaveStartPostWaitMilliseconds:               1000,
		DiscoverByShowSlaveHosts:                     false,
		DiscoveryPollSeconds:                         5,
		InstanceBulkOperationsWaitTimeoutSeconds:     10,
		ActiveNodeExpireSeconds:                      60,
		HostnameResolveMethod:                        "cname",
		MySQLHostnameResolveMethod:                   "@@hostname",
		SkipBinlogServerUnresolveCheck:               true,
		ExpiryHostnameResolvesMinutes:                60,
		RejectHostnameResolvePattern:                 "",
		ReasonableReplicationLagSeconds:              10,
		ProblemIgnoreHostnameFilters:                 []string{},
		VerifyReplicationFilters:                     false,
		MaintenanceOwner:                             "bakufu",
		ReasonableMaintenanceReplicationLagSeconds:   20,
		MaintenanceExpireMinutes:                     10,
		MaintenancePurgeDays:                         365,
		CandidateInstanceExpireMinutes:               60,
		AuditLogFile:                                 "",
		AuditToSyslog:                                false,
		AuditPageSize:                                20,
		AuditPurgeDays:                               365,
		RemoveTextFromHostnameDisplay:                "",
		ReadOnly:                                     false,
		AuthenticationMethod:                         "basic",
		HTTPAuthUser:                                 "",
		HTTPAuthPassword:                             "",
		AuthUserHeader:                               "X-Forwarded-User",
		PowerAuthUsers:                               []string{"*"},
		AccessTokenUseExpirySeconds:                  60,
		AccessTokenExpiryMinutes:                     1440,
		ClusterNameToAlias:                           make(map[string]string),
		DetectClusterAliasQuery:                      "",
		DetectClusterDomainQuery:                     "",
		DataCenterPattern:                            "",
		PhysicalEnvironmentPattern:                   "",
		DetectDataCenterQuery:                        "",
		DetectPhysicalEnvironmentQuery:               "",
		SupportFuzzyPoolHostnames:                    true,
		PromotionIgnoreHostnameFilters:               []string{},
		ServeAgentsHttp:                              false,
		AgentsUseSSL:                                 false,
		AgentsUseMutualTLS:                           false,
		AgentSSLValidOUs:                             []string{},
		AgentSSLSkipVerify:                           false,
		AgentSSLPrivateKeyFile:                       "",
		AgentSSLCertFile:                             "",
		AgentSSLCAFile:                               "",
		UseSSL:                                       false,
		UseMutualTLS:                                 false,
		SSLValidOUs:                                  []string{},
		SSLSkipVerify:                                false,
		SSLPrivateKeyFile:                            "",
		SSLCertFile:                                  "",
		SSLCAFile:                                    "",
		HttpTimeoutSeconds:                           60,
		AgentPollMinutes:                             60,
		AgentAutoDiscover:                            false,
		UnseenAgentForgetHours:                       6,
		StaleSeedFailMinutes:                         60,
		SeedAcceptableBytesDiff:                      8192,
		PseudoGTIDPattern:                            "",
		PseudoGTIDPatternIsFixedSubstring:            false,
		PseudoGTIDMonotonicHint:                      "",
		DetectPseudoGTIDQuery:                        "",
		PseudoGTIDCoordinatesHistoryHeuristicMinutes: 2,
		BinlogEventsChunkSize:                        10000,
		BufferBinlogEvents:                           true,
		SkipBinlogEventsContaining:                   []string{},
		ReduceReplicationAnalysisCount:               true,
		FailureDetectionPeriodBlockMinutes:           60,
		RecoveryPollSeconds:                          10,
		RecoveryPeriodBlockMinutes:                   60,
		RecoveryPeriodBlockSeconds:                   0,
		RecoveryIgnoreHostnameFilters:                []string{},
		RecoverMasterClusterFilters:                  []string{},
		RecoverIntermediateMasterClusterFilters:      []string{},
		OnFailureDetectionProcesses:                  []string{},
		PreFailoverProcesses:                         []string{},
		PostMasterFailoverProcesses:                  []string{},
		PostIntermediateMasterFailoverProcesses:      []string{},
		PostFailoverProcesses:                        []string{},
		PostUnsuccessfulFailoverProcesses:            []string{},
		CoMasterRecoveryMustPromoteOtherCoMaster:     true,
		DetachLostSlavesAfterMasterFailover:          true,
		ApplyMySQLPromotionAfterMasterFailover:       false,
		MasterFailoverLostInstancesDowntimeMinutes:   0,
		PostponeSlaveRecoveryOnLagMinutes:            0,
		OSCIgnoreHostnameFilters:                     []string{},
		GraphiteAddr:                                 "",
		GraphitePath:                                 "",
		GraphiteConvertHostnameDotsToUnderscores:     true,
		GraphitePollSeconds:                          60,
	}
}

func postReadAdjustments() {
	if Config.MySQLBakufuCredentialsConfigFile != "" {
		mySQLConfig := struct {
			Client struct {
				User     string
				Password string
			}
		}{}
		err := gcfg.ReadFileInto(&mySQLConfig, Config.MySQLBakufuCredentialsConfigFile)
		if err != nil {
			log.Fatalf("Failed to parse gcfg data from file: %+v", err)
		} else {
			log.Debugf("Parsed bakufu credentials from %s", Config.MySQLBakufuCredentialsConfigFile)
			Config.MySQLBakufuUser = mySQLConfig.Client.User
			Config.MySQLBakufuPassword = mySQLConfig.Client.Password
		}
	}
	{
		submatch := envVariableRegexp.FindStringSubmatch(Config.MySQLBakufuPassword)
		if len(submatch) > 1 {
			Config.MySQLBakufuPassword = os.Getenv(submatch[1])
		}
	}
	if Config.MySQLTopologyCredentialsConfigFile != "" {
		mySQLConfig := struct {
			Client struct {
				User     string
				Password string
			}
		}{}
		err := gcfg.ReadFileInto(&mySQLConfig, Config.MySQLTopologyCredentialsConfigFile)
		if err != nil {
			log.Fatalf("Failed to parse gcfg data from file: %+v", err)
		} else {
			log.Debugf("Parsed topology credentials from %s", Config.MySQLTopologyCredentialsConfigFile)
			Config.MySQLTopologyUser = mySQLConfig.Client.User
			Config.MySQLTopologyPassword = mySQLConfig.Client.Password
		}
	}
	{
		submatch := envVariableRegexp.FindStringSubmatch(Config.MySQLTopologyPassword)
		if len(submatch) > 1 {
			Config.MySQLTopologyPassword = os.Getenv(submatch[1])
		}
	}

	if Config.RecoveryPeriodBlockSeconds == 0 && Config.RecoveryPeriodBlockMinutes > 0 {
		Config.RecoveryPeriodBlockSeconds = Config.RecoveryPeriodBlockMinutes * 60
	}
}

func read(fileName string) (*Configuration, error) {
	file, err := os.Open(fileName)
	if err == nil {
		decoder := json.NewDecoder(file)
		err := decoder.Decode(Config)
		if err == nil {
			log.Infof("Read config: %s", fileName)
		} else {
			log.Fatal("Cannot read config file:", fileName, err)
		}
		postReadAdjustments()
	}
	return Config, err
}

func Read(fileNames ...string) *Configuration {
	for _, fileName := range fileNames {
		read(fileName)
	}
	readFileNames = fileNames
	return Config
}

func ForceRead(fileName string) *Configuration {
	_, err := read(fileName)
	if err != nil {
		log.Fatal("Cannot read config file:", fileName, err)
	}
	readFileNames = []string{fileName}
	return Config
}

func Reload() *Configuration {
	for _, fileName := range readFileNames {
		read(fileName)
	}
	return Config
}
