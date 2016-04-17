/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package agent

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/stnmrshx/golib/log"
	"github.com/stnmrshx/golib/sqlutils"
	"github.com/stnmrshx/bakufu/go/config"
	"github.com/stnmrshx/bakufu/go/db"
	"github.com/stnmrshx/bakufu/go/inst"
)

var SeededAgents chan *Agent = make(chan *Agent)

var httpTimeout = time.Duration(time.Duration(config.Config.HttpTimeoutSeconds) * time.Second)

func dialTimeout(network, addr string) (net.Conn, error) {
	return net.DialTimeout(network, addr, httpTimeout)
}

var httpTransport = &http.Transport{
	TLSClientConfig: &tls.Config{InsecureSkipVerify: config.Config.AgentSSLSkipVerify},
	Dial:            dialTimeout,
	ResponseHeaderTimeout: httpTimeout,
}
var httpClient = &http.Client{Transport: httpTransport}

func httpGet(url string) (resp *http.Response, err error) {
	return httpClient.Get(url)
}

func auditAgentOperation(auditType string, agent *Agent, message string) error {

	instanceKey := &inst.InstanceKey{}
	if agent != nil {
		instanceKey = &inst.InstanceKey{Hostname: agent.Hostname, Port: int(agent.MySQLPort)}
	}
	return inst.AuditOperation(auditType, instanceKey, message)
}

func readResponse(res *http.Response, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	if res.Status == "500" {
		return body, errors.New("Response Status 500")
	}

	return body, nil
}

func SubmitAgent(hostname string, port int, token string) (string, error) {
	_, err := db.ExecBakufu(`
			replace 
				into host_agent (
					hostname, port, token, last_submitted
				) VALUES (
					?, ?, ?, NOW()
				)
			`,
		hostname,
		port,
		token,
	)
	if err != nil {
		return "", log.Errore(err)
	}

	if config.Config.AgentAutoDiscover {
		DiscoverAgentInstance(hostname, port)
	}

	return hostname, err
}

func DiscoverAgentInstance(hostname string, port int) error {
	agent, err := GetAgent(hostname)
	if err != nil {
		log.Errorf("Couldn't get agent for %s: %v", hostname, err)
		return err
	}

	instanceKey := agent.GetInstance()
	instance, err := inst.ReadTopologyInstance(instanceKey)
	if err != nil {
		log.Errorf("Failed to read topology for %v", instanceKey)
		return err
	}
	log.Infof("Discovered Agent Instance: %v", instance.Key)
	return nil
}

func ForgetLongUnseenAgents() error {
	_, err := db.ExecBakufu(`
			delete 
				from host_agent 
			where 
				last_submitted < NOW() - interval ? hour`,
		config.Config.UnseenAgentForgetHours,
	)
	return err
}

func ReadOutdatedAgentsHosts() ([]string, error) {
	res := []string{}
	query := `
		select 
			hostname 
		from 
			host_agent 
		where
			IFNULL(last_checked < now() - interval ? minute, true)
			`
	err := db.QueryBakufu(query, sqlutils.Args(config.Config.AgentPollMinutes), func(m sqlutils.RowMap) error {
		hostname := m.GetString("hostname")
		res = append(res, hostname)
		return nil
	})

	if err != nil {
		log.Errore(err)
	}
	return res, err
}

func ReadAgents() ([]Agent, error) {
	res := []Agent{}
	query := `
		select 
			hostname,
			port,
			token,
			last_submitted,
			mysql_port
		from 
			host_agent
		order by
			hostname
		`
	err := db.QueryBakufuRowsMap(query, func(m sqlutils.RowMap) error {
		agent := Agent{}
		agent.Hostname = m.GetString("hostname")
		agent.Port = m.GetInt("port")
		agent.MySQLPort = m.GetInt64("mysql_port")
		agent.Token = ""
		agent.LastSubmitted = m.GetString("last_submitted")

		res = append(res, agent)
		return nil
	})

	if err != nil {
		log.Errore(err)
	}
	return res, err

}

func readAgentBasicInfo(hostname string) (Agent, string, error) {
	agent := Agent{}
	token := ""
	query := `
		select 
			hostname,
			port,
			token,
			last_submitted,
			mysql_port
		from 
			host_agent
		where
			hostname = ?
		`
	err := db.QueryBakufu(query, sqlutils.Args(hostname), func(m sqlutils.RowMap) error {
		agent.Hostname = m.GetString("hostname")
		agent.Port = m.GetInt("port")
		agent.LastSubmitted = m.GetString("last_submitted")
		agent.MySQLPort = m.GetInt64("mysql_port")
		token = m.GetString("token")

		return nil
	})
	if err != nil {
		return agent, "", err
	}

	if token == "" {
		return agent, "", log.Errorf("Cannot get agent/token: %s", hostname)
	}
	return agent, token, nil
}

func UpdateAgentLastChecked(hostname string) error {
	_, err := db.ExecBakufu(`
        	update 
        		host_agent 
        	set
        		last_checked = NOW()
			where 
				hostname = ?`,
		hostname,
	)
	if err != nil {
		return log.Errore(err)
	}

	return nil
}

func UpdateAgentInfo(hostname string, agent Agent) error {
	_, err := db.ExecBakufu(`
        	update 
        		host_agent 
        	set
        		last_seen = NOW(),
        		mysql_port = ?,
        		count_mysql_snapshots = ?
			where 
				hostname = ?`,
		agent.MySQLPort,
		len(agent.LogicalVolumes),
		hostname,
	)
	if err != nil {
		return log.Errore(err)
	}

	return nil
}

func baseAgentUri(agentHostname string, agentPort int) string {
	protocol := "http"
	if config.Config.AgentsUseSSL {
		protocol = "https"
	}
	uri := fmt.Sprintf("%s://%s:%d/api", protocol, agentHostname, agentPort)
	log.Debugf("bakufu-agent uri: %s", uri)
	return uri
}

func GetAgent(hostname string) (Agent, error) {
	agent, token, err := readAgentBasicInfo(hostname)
	if err != nil {
		return agent, log.Errore(err)
	}

	{
		uri := baseAgentUri(agent.Hostname, agent.Port)
		log.Debugf("bakufu-agent uri: %s", uri)

		{
			availableLocalSnapshotsUri := fmt.Sprintf("%s/available-snapshots-local?token=%s", uri, token)
			body, err := readResponse(httpGet(availableLocalSnapshotsUri))
			if err == nil {
				err = json.Unmarshal(body, &agent.AvailableLocalSnapshots)
			}
			if err != nil {
				log.Errore(err)
			}
		}
		{
			availableSnapshotsUri := fmt.Sprintf("%s/available-snapshots?token=%s", uri, token)
			body, err := readResponse(httpGet(availableSnapshotsUri))
			if err == nil {
				err = json.Unmarshal(body, &agent.AvailableSnapshots)
			}
			if err != nil {
				log.Errore(err)
			}
		}
		{
			lvSnapshotsUri := fmt.Sprintf("%s/lvs-snapshots?token=%s", uri, token)
			body, err := readResponse(httpGet(lvSnapshotsUri))
			if err == nil {
				err = json.Unmarshal(body, &agent.LogicalVolumes)
			}
			if err != nil {
				log.Errore(err)
			}
		}
		{
			mountUri := fmt.Sprintf("%s/mount?token=%s", uri, token)
			body, err := readResponse(httpGet(mountUri))
			if err == nil {
				err = json.Unmarshal(body, &agent.MountPoint)
			}
			if err != nil {
				log.Errore(err)
			}
		}
		{
			mySQLRunningUri := fmt.Sprintf("%s/mysql-status?token=%s", uri, token)
			body, err := readResponse(httpGet(mySQLRunningUri))
			if err == nil {
				err = json.Unmarshal(body, &agent.MySQLRunning)
			}
		}
		{
			mySQLRunningUri := fmt.Sprintf("%s/mysql-port?token=%s", uri, token)
			body, err := readResponse(httpGet(mySQLRunningUri))
			if err == nil {
				err = json.Unmarshal(body, &agent.MySQLPort)
			}
			if err != nil {
				log.Errore(err)
			}
		}
		{
			mySQLDiskUsageUri := fmt.Sprintf("%s/mysql-du?token=%s", uri, token)
			body, err := readResponse(httpGet(mySQLDiskUsageUri))
			if err == nil {
				err = json.Unmarshal(body, &agent.MySQLDiskUsage)
			}
			if err != nil {
				log.Errore(err)
			}
		}
		{
			mySQLDatadirDiskFreeUri := fmt.Sprintf("%s/mysql-datadir-available-space?token=%s", uri, token)
			body, err := readResponse(httpGet(mySQLDatadirDiskFreeUri))
			if err == nil {
				err = json.Unmarshal(body, &agent.MySQLDatadirDiskFree)
			}
			if err != nil {
				log.Errore(err)
			}
		}
		{
			errorLogTailUri := fmt.Sprintf("%s/mysql-error-log-tail?token=%s", uri, token)
			body, err := readResponse(httpGet(errorLogTailUri))
			if err == nil {
				err = json.Unmarshal(body, &agent.MySQLErrorLogTail)
			}
			if err != nil {
				log.Errore(err)
			}
		}
	}
	return agent, err
}

func executeAgentCommand(hostname string, command string, onResponse *func([]byte)) (Agent, error) {
	agent, token, err := readAgentBasicInfo(hostname)
	if err != nil {
		return agent, err
	}

	uri := baseAgentUri(agent.Hostname, agent.Port)

	var fullCommand string
	if strings.Contains(command, "?") {
		fullCommand = fmt.Sprintf("%s&token=%s", command, token)
	} else {
		fullCommand = fmt.Sprintf("%s?token=%s", command, token)
	}
	log.Debugf("bakufu-agent command: %s", fullCommand)
	agentCommandUri := fmt.Sprintf("%s/%s", uri, fullCommand)

	body, err := readResponse(httpGet(agentCommandUri))
	if err != nil {
		return agent, log.Errore(err)
	}
	if onResponse != nil {
		(*onResponse)(body)
	}
	auditAgentOperation("agent-command", &agent, command)

	return agent, err
}

func Unmount(hostname string) (Agent, error) {
	return executeAgentCommand(hostname, "umount", nil)
}

func MountLV(hostname string, lv string) (Agent, error) {
	return executeAgentCommand(hostname, fmt.Sprintf("mountlv?lv=%s", lv), nil)
}

func RemoveLV(hostname string, lv string) (Agent, error) {
	return executeAgentCommand(hostname, fmt.Sprintf("removelv?lv=%s", lv), nil)
}

func CreateSnapshot(hostname string) (Agent, error) {
	return executeAgentCommand(hostname, "create-snapshot", nil)
}

func deleteMySQLDatadir(hostname string) (Agent, error) {
	return executeAgentCommand(hostname, "delete-mysql-datadir", nil)
}

func MySQLStop(hostname string) (Agent, error) {
	return executeAgentCommand(hostname, "mysql-stop", nil)
}

func MySQLStart(hostname string) (Agent, error) {
	return executeAgentCommand(hostname, "mysql-start", nil)
}

func ReceiveMySQLSeedData(hostname string, seedId int64) (Agent, error) {
	return executeAgentCommand(hostname, fmt.Sprintf("receive-mysql-seed-data/%d", seedId), nil)
}

func SendMySQLSeedData(hostname string, targetHostname string, seedId int64) (Agent, error) {
	return executeAgentCommand(hostname, fmt.Sprintf("send-mysql-seed-data/%s/%d", targetHostname, seedId), nil)
}

func AbortSeedCommand(hostname string, seedId int64) (Agent, error) {
	return executeAgentCommand(hostname, fmt.Sprintf("abort-seed/%d", seedId), nil)
}

func seedCommandCompleted(hostname string, seedId int64) (Agent, bool, error) {
	result := false
	onResponse := func(body []byte) {
		json.Unmarshal(body, &result)
	}
	agent, err := executeAgentCommand(hostname, fmt.Sprintf("seed-command-completed/%d", seedId), &onResponse)
	return agent, result, err
}

func seedCommandSucceeded(hostname string, seedId int64) (Agent, bool, error) {
	result := false
	onResponse := func(body []byte) {
		json.Unmarshal(body, &result)
	}
	agent, err := executeAgentCommand(hostname, fmt.Sprintf("seed-command-succeeded/%d", seedId), &onResponse)
	return agent, result, err
}

func AbortSeed(seedId int64) error {
	seedOperations, err := AgentSeedDetails(seedId)
	if err != nil {
		return log.Errore(err)
	}

	for _, seedOperation := range seedOperations {
		AbortSeedCommand(seedOperation.TargetHostname, seedId)
		AbortSeedCommand(seedOperation.SourceHostname, seedId)
	}
	updateSeedComplete(seedId, errors.New("Aborted"))
	return nil
}

func PostCopy(hostname string) (Agent, error) {
	return executeAgentCommand(hostname, "post-copy", nil)
}

func SubmitSeedEntry(targetHostname string, sourceHostname string) (int64, error) {
	res, err := db.ExecBakufu(`
			insert 
				into agent_seed (
					target_hostname, source_hostname, start_timestamp
				) VALUES (
					?, ?, NOW()
				)
			`,
		targetHostname,
		sourceHostname,
	)
	if err != nil {
		return 0, log.Errore(err)
	}
	id, err := res.LastInsertId()

	return id, err
}

func updateSeedComplete(seedId int64, seedError error) error {
	_, err := db.ExecBakufu(`
			update 
				agent_seed
					set end_timestamp = NOW(),
					is_complete = 1,
					is_successful = ?
				where
					agent_seed_id = ?
			`,
		(seedError == nil),
		seedId,
	)
	if err != nil {
		return log.Errore(err)
	}

	return nil
}

func submitSeedStateEntry(seedId int64, action string, errorMessage string) (int64, error) {
	res, err := db.ExecBakufu(`
			insert 
				into agent_seed_state (
					agent_seed_id, state_timestamp, state_action, error_message
				) VALUES (
					?, NOW(), ?, ?
				)
			`,
		seedId,
		action,
		errorMessage,
	)
	if err != nil {
		return 0, log.Errore(err)
	}
	id, err := res.LastInsertId()

	return id, err
}

func updateSeedStateEntry(seedStateId int64, reason error) error {
	_, err := db.ExecBakufu(`
			update 
				agent_seed_state
					set error_message = ?
				where
					agent_seed_state_id = ?
			`,
		reason.Error(),
		seedStateId,
	)
	if err != nil {
		return log.Errore(err)
	}

	return reason
}

func FailStaleSeeds() error {
	_, err := db.ExecBakufu(`
				update 
						agent_seed 
					set 
						is_complete=1, 
						is_successful=0 
					where 
						is_complete=0 
						and (
							select 
									max(state_timestamp) as last_state_timestamp 
								from 
									agent_seed_state 
								where 
									agent_seed.agent_seed_id = agent_seed_state.agent_seed_id
						) < now() - interval ? minute`,
		config.Config.StaleSeedFailMinutes,
	)
	return err
}

func executeSeed(seedId int64, targetHostname string, sourceHostname string) error {

	var err error
	var seedStateId int64

	seedStateId, _ = submitSeedStateEntry(seedId, fmt.Sprintf("getting target agent info for %s", targetHostname), "")
	targetAgent, err := GetAgent(targetHostname)
	SeededAgents <- &targetAgent
	if err != nil {
		return updateSeedStateEntry(seedStateId, err)
	}

	seedStateId, _ = submitSeedStateEntry(seedId, fmt.Sprintf("getting source agent info for %s", sourceHostname), "")
	sourceAgent, err := GetAgent(sourceHostname)
	if err != nil {
		return updateSeedStateEntry(seedStateId, err)
	}

	seedStateId, _ = submitSeedStateEntry(seedId, fmt.Sprintf("Checking Percona status on target %s", targetHostname), "")
	if targetAgent.MySQLRunning {
		return updateSeedStateEntry(seedStateId, errors.New("Percona is running on target host. Cowardly refusing to proceeed. Please stop the Percona service"))
	}

	seedStateId, _ = submitSeedStateEntry(seedId, fmt.Sprintf("Looking up available snapshots on source %s", sourceHostname), "")
	if len(sourceAgent.LogicalVolumes) == 0 {
		return updateSeedStateEntry(seedStateId, errors.New("No logical volumes found on source host"))
	}

	seedStateId, _ = submitSeedStateEntry(seedId, fmt.Sprintf("Checking mount point on source %s", sourceHostname), "")
	if sourceAgent.MountPoint.IsMounted {
		return updateSeedStateEntry(seedStateId, errors.New("Volume already mounted on source host; please unmount"))
	}

	seedFromLogicalVolume := sourceAgent.LogicalVolumes[0]
	seedStateId, _ = submitSeedStateEntry(seedId, fmt.Sprintf("Mounting logical volume: %s", seedFromLogicalVolume.Path), "")
	_, err = MountLV(sourceHostname, seedFromLogicalVolume.Path)
	if err != nil {
		return updateSeedStateEntry(seedStateId, err)
	}
	sourceAgent, err = GetAgent(sourceHostname)
	seedStateId, _ = submitSeedStateEntry(seedId, fmt.Sprintf("Percona data volume on source host %s is %d bytes", sourceHostname, sourceAgent.MountPoint.MySQLDiskUsage), "")

	seedStateId, _ = submitSeedStateEntry(seedId, fmt.Sprintf("Erasing Percona data on %s", targetHostname), "")
	_, err = deleteMySQLDatadir(targetHostname)
	if err != nil {
		return updateSeedStateEntry(seedStateId, err)
	}

	seedStateId, _ = submitSeedStateEntry(seedId, fmt.Sprintf("Aquiring target host datadir free space on %s", targetHostname), "")
	targetAgent, err = GetAgent(targetHostname)
	if err != nil {
		return updateSeedStateEntry(seedStateId, err)
	}

	if sourceAgent.MountPoint.MySQLDiskUsage > targetAgent.MySQLDatadirDiskFree {
		Unmount(sourceHostname)
		return updateSeedStateEntry(seedStateId, fmt.Errorf("Not enough disk space on target host %s. Required: %d, available: %d. Bailing out.", targetHostname, sourceAgent.MountPoint.MySQLDiskUsage, targetAgent.MySQLDatadirDiskFree))
	}

	seedStateId, _ = submitSeedStateEntry(seedId, fmt.Sprintf("%s will now receive data in background", targetHostname), "")
	ReceiveMySQLSeedData(targetHostname, seedId)

	seedStateId, _ = submitSeedStateEntry(seedId, fmt.Sprintf("Waiting some time for %s to start listening for incoming data", targetHostname), "")
	time.Sleep(2 * time.Second)

	seedStateId, _ = submitSeedStateEntry(seedId, fmt.Sprintf("%s will now send data to %s in background", sourceHostname, targetHostname), "")
	SendMySQLSeedData(sourceHostname, targetHostname, seedId)

	copyComplete := false
	numStaleIterations := 0
	var bytesCopied int64 = 0

	for !copyComplete {
		targetAgentPoll, err := GetAgent(targetHostname)
		if err != nil {
			return log.Errore(err)
		}

		if targetAgentPoll.MySQLDiskUsage == bytesCopied {
			numStaleIterations++
		}
		bytesCopied = targetAgentPoll.MySQLDiskUsage

		copyFailed := false
		if _, commandCompleted, _ := seedCommandCompleted(targetHostname, seedId); commandCompleted {
			copyComplete = true
			if _, commandSucceeded, _ := seedCommandSucceeded(targetHostname, seedId); !commandSucceeded {
				copyFailed = true
			}
		}
		if numStaleIterations > 10 {
			copyFailed = true
		}
		if copyFailed {
			AbortSeedCommand(sourceHostname, seedId)
			AbortSeedCommand(targetHostname, seedId)
			Unmount(sourceHostname)
			return updateSeedStateEntry(seedStateId, errors.New("10 iterations have passed without progress. Bailing out."))
		}

		var copyPct int64 = 0
		if sourceAgent.MountPoint.MySQLDiskUsage > 0 {
			copyPct = 100 * bytesCopied / sourceAgent.MountPoint.MySQLDiskUsage
		}
		seedStateId, _ = submitSeedStateEntry(seedId, fmt.Sprintf("Copied %d/%d bytes (%d%%)", bytesCopied, sourceAgent.MountPoint.MySQLDiskUsage, copyPct), "")

		if !copyComplete {
			time.Sleep(30 * time.Second)
		}
	}

	seedStateId, _ = submitSeedStateEntry(seedId, fmt.Sprintf("Executing post-copy command on %s", targetHostname), "")
	_, err = PostCopy(targetHostname)
	if err != nil {
		return updateSeedStateEntry(seedStateId, err)
	}

	seedStateId, _ = submitSeedStateEntry(seedId, fmt.Sprintf("Unmounting logical volume: %s", seedFromLogicalVolume.Path), "")
	_, err = Unmount(sourceHostname)
	if err != nil {
		return updateSeedStateEntry(seedStateId, err)
	}

	seedStateId, _ = submitSeedStateEntry(seedId, fmt.Sprintf("Starting Percona on target: %s", targetHostname), "")
	_, err = MySQLStart(targetHostname)
	if err != nil {
		return updateSeedStateEntry(seedStateId, err)
	}

	seedStateId, _ = submitSeedStateEntry(seedId, fmt.Sprintf("Submitting Percona instance for discovery: %s", targetHostname), "")
	SeededAgents <- &targetAgent

	seedStateId, _ = submitSeedStateEntry(seedId, "Done", "")

	return nil
}

func Seed(targetHostname string, sourceHostname string) (int64, error) {
	if targetHostname == sourceHostname {
		return 0, log.Errorf("Cannot seed %s onto itself", targetHostname)
	}
	seedId, err := SubmitSeedEntry(targetHostname, sourceHostname)
	if err != nil {
		return 0, log.Errore(err)
	}

	go func() {
		err := executeSeed(seedId, targetHostname, sourceHostname)
		updateSeedComplete(seedId, err)
	}()

	return seedId, nil
}

func readSeeds(whereCondition string, args []interface{}, limit string) ([]SeedOperation, error) {
	res := []SeedOperation{}
	query := fmt.Sprintf(`
		select 
			agent_seed_id,
			target_hostname,
			source_hostname,
			start_timestamp,
			end_timestamp,
			is_complete,
			is_successful
		from 
			agent_seed
		%s
		order by
			agent_seed_id desc
		%s
		`, whereCondition, limit)
	err := db.QueryBakufu(query, args, func(m sqlutils.RowMap) error {
		seedOperation := SeedOperation{}
		seedOperation.SeedId = m.GetInt64("agent_seed_id")
		seedOperation.TargetHostname = m.GetString("target_hostname")
		seedOperation.SourceHostname = m.GetString("source_hostname")
		seedOperation.StartTimestamp = m.GetString("start_timestamp")
		seedOperation.EndTimestamp = m.GetString("end_timestamp")
		seedOperation.IsComplete = m.GetBool("is_complete")
		seedOperation.IsSuccessful = m.GetBool("is_successful")

		res = append(res, seedOperation)
		return nil
	})

	if err != nil {
		log.Errore(err)
	}
	return res, err
}

func ReadActiveSeedsForHost(hostname string) ([]SeedOperation, error) {
	whereCondition := `
		where
			is_complete = 0
			and (
				target_hostname = ?
				or source_hostname = ?
			)
		`
	return readSeeds(whereCondition, sqlutils.Args(hostname, hostname), "")
}

func ReadRecentCompletedSeedsForHost(hostname string) ([]SeedOperation, error) {
	whereCondition := `
		where
			is_complete = 1
			and (
				target_hostname = ?
				or source_hostname = ?
			)
		`
	return readSeeds(whereCondition, sqlutils.Args(hostname, hostname), "limit 10")
}

func AgentSeedDetails(seedId int64) ([]SeedOperation, error) {
	whereCondition := `
		where
			agent_seed_id = ?
		`
	return readSeeds(whereCondition, sqlutils.Args(seedId), "")
}

func ReadRecentSeeds() ([]SeedOperation, error) {
	return readSeeds(``, sqlutils.Args(), "limit 100")
}

func ReadSeedStates(seedId int64) ([]SeedOperationState, error) {
	res := []SeedOperationState{}
	query := `
		select 
			agent_seed_state_id,
			agent_seed_id,
			state_timestamp,
			state_action,
			error_message
		from 
			agent_seed_state
		where
			agent_seed_id = ?
		order by
			agent_seed_state_id desc
		`
	err := db.QueryBakufu(query, sqlutils.Args(seedId), func(m sqlutils.RowMap) error {
		seedState := SeedOperationState{}
		seedState.SeedStateId = m.GetInt64("agent_seed_state_id")
		seedState.SeedId = m.GetInt64("agent_seed_id")
		seedState.StateTimestamp = m.GetString("state_timestamp")
		seedState.Action = m.GetString("state_action")
		seedState.ErrorMessage = m.GetString("error_message")

		res = append(res, seedState)
		return nil
	})

	if err != nil {
		log.Errore(err)
	}
	return res, err
}
