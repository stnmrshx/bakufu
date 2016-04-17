/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package inst

import (
	"fmt"
	"github.com/stnmrshx/bakufu/go/config"
	"github.com/stnmrshx/bakufu/go/db"
	"github.com/stnmrshx/bakufu/go/inst"
	"github.com/stnmrshx/bakufu/go/logic"
	. "gopkg.in/check.v1"
	"math/rand"
	"testing"
	"time"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct{}

var _ = Suite(&TestSuite{})

var masterKey = inst.InstanceKey{
	Hostname: "127.0.0.1",
	Port:     22987,
}
var slave1Key = inst.InstanceKey{
	Hostname: "127.0.0.1",
	Port:     22988,
}
var slave2Key = inst.InstanceKey{
	Hostname: "127.0.0.1",
	Port:     22989,
}
var slave3Key = inst.InstanceKey{
	Hostname: "127.0.0.1",
	Port:     22990,
}

func clearTestMaintenance() {
	_, _ = db.ExecBakufu("update database_instance_maintenance set maintenance_active=null, end_timestamp=NOW() where owner = ?", "unittest")
}

func (s *TestSuite) SetUpSuite(c *C) {
	config.Config.MySQLTopologyUser = "msandbox"
	config.Config.MySQLTopologyPassword = "msandbox"
	config.Config.MySQLBakufuHost = "127.0.0.1"
	config.Config.MySQLBakufuPort = 5622
	config.Config.MySQLBakufuDatabase = "bakufu"
	config.Config.MySQLBakufuUser = "msandbox"
	config.Config.MySQLBakufuPassword = "msandbox"
	config.Config.DiscoverByShowSlaveHosts = true

	_, _ = db.ExecBakufu("delete from database_instance where hostname = ? and port = ?", masterKey.Hostname, masterKey.Port)
	_, _ = db.ExecBakufu("delete from database_instance where hostname = ? and port = ?", slave1Key.Hostname, slave1Key.Port)
	_, _ = db.ExecBakufu("delete from database_instance where hostname = ? and port = ?", slave2Key.Hostname, slave2Key.Port)
	_, _ = db.ExecBakufu("delete from database_instance where hostname = ? and port = ?", slave3Key.Hostname, slave3Key.Port)

	inst.ExecInstance(&masterKey, "drop database if exists bakufu_test")
	inst.ExecInstance(&masterKey, "create database bakufu_test")
	inst.ExecInstance(&masterKey, `create table bakufu_test.test_table(
			name    varchar(128) charset ascii not null primary key,
			value   varchar(128) charset ascii not null
		)`)
	rand.Seed(time.Now().UTC().UnixNano())
}

func (s *TestSuite) TestReadTopologyMaster(c *C) {
	key := masterKey
	i, _ := inst.ReadTopologyInstance(&key)

	c.Assert(i.Key.Hostname, Equals, key.Hostname)
	c.Assert(i.IsSlave(), Equals, false)
	c.Assert(len(i.SlaveHosts), Equals, 3)
	c.Assert(len(i.SlaveHosts.GetInstanceKeys()), Equals, len(i.SlaveHosts))
}

func (s *TestSuite) TestReadTopologySlave(c *C) {
	key := slave3Key
	i, _ := inst.ReadTopologyInstance(&key)
	c.Assert(i.Key.Hostname, Equals, key.Hostname)
	c.Assert(i.IsSlave(), Equals, true)
	c.Assert(len(i.SlaveHosts), Equals, 0)
}

func (s *TestSuite) TestReadTopologyAndInstanceMaster(c *C) {
	i, _ := inst.ReadTopologyInstance(&masterKey)
	iRead, found, _ := inst.ReadInstance(&masterKey)
	c.Assert(found, Equals, true)
	c.Assert(iRead.Key.Hostname, Equals, i.Key.Hostname)
	c.Assert(iRead.Version, Equals, i.Version)
	c.Assert(len(iRead.SlaveHosts), Equals, len(i.SlaveHosts))
}

func (s *TestSuite) TestReadTopologyAndInstanceSlave(c *C) {
	i, _ := inst.ReadTopologyInstance(&slave1Key)
	iRead, found, _ := inst.ReadInstance(&slave1Key)
	c.Assert(found, Equals, true)
	c.Assert(iRead.Key.Hostname, Equals, i.Key.Hostname)
	c.Assert(iRead.Version, Equals, i.Version)
}

func (s *TestSuite) TestGetMasterOfASlave(c *C) {
	i, err := inst.ReadTopologyInstance(&slave1Key)
	c.Assert(err, IsNil)
	master, err := inst.GetInstanceMaster(i)
	c.Assert(err, IsNil)
	c.Assert(master.IsSlave(), Equals, false)
	c.Assert(master.Key.Port, Equals, 22987)
}

func (s *TestSuite) TestSlavesAreSiblings(c *C) {
	i0, _ := inst.ReadTopologyInstance(&slave1Key)
	i1, _ := inst.ReadTopologyInstance(&slave2Key)
	c.Assert(inst.InstancesAreSiblings(i0, i1), Equals, true)
}

func (s *TestSuite) TestNonSiblings(c *C) {
	i0, _ := inst.ReadTopologyInstance(&masterKey)
	i1, _ := inst.ReadTopologyInstance(&slave1Key)
	c.Assert(inst.InstancesAreSiblings(i0, i1), Not(Equals), true)
}

func (s *TestSuite) TestInstanceIsMasterOf(c *C) {
	i0, _ := inst.ReadTopologyInstance(&masterKey)
	i1, _ := inst.ReadTopologyInstance(&slave1Key)
	c.Assert(inst.InstanceIsMasterOf(i0, i1), Equals, true)
}

func (s *TestSuite) TestStopStartSlave(c *C) {

	i, _ := inst.ReadTopologyInstance(&slave1Key)
	c.Assert(i.SlaveRunning(), Equals, true)
	i, _ = inst.StopSlaveNicely(&i.Key, 0)

	c.Assert(i.SlaveRunning(), Equals, false)
	c.Assert(i.SQLThreadUpToDate(), Equals, true)

	i, _ = inst.StartSlave(&i.Key)
	c.Assert(i.SlaveRunning(), Equals, true)
}

func (s *TestSuite) TestReadTopologyUnexisting(c *C) {
	key := inst.InstanceKey{
		Hostname: "127.0.0.1",
		Port:     22999,
	}
	_, err := inst.ReadTopologyInstance(&key)

	c.Assert(err, Not(IsNil))
}

func (s *TestSuite) TestMoveBelowAndBack(c *C) {
	clearTestMaintenance()
	slave1, err := inst.MoveBelow(&slave1Key, &slave2Key)
	c.Assert(err, IsNil)

	c.Assert(slave1.MasterKey.Equals(&slave2Key), Equals, true)
	c.Assert(slave1.SlaveRunning(), Equals, true)

	slave1, _ = inst.MoveUp(&slave1Key)
	slave2, _ := inst.ReadTopologyInstance(&slave2Key)

	c.Assert(inst.InstancesAreSiblings(slave1, slave2), Equals, true)
	c.Assert(slave1.SlaveRunning(), Equals, true)

}

func (s *TestSuite) TestMoveBelowAndBackComplex(c *C) {
	clearTestMaintenance()

	slave1, _ := inst.MoveBelow(&slave1Key, &slave2Key)

	c.Assert(slave1.MasterKey.Equals(&slave2Key), Equals, true)
	c.Assert(slave1.SlaveRunning(), Equals, true)

	_, err := inst.StopSlave(&slave2Key)
	c.Assert(err, IsNil)

	randValue := rand.Int()
	_, err = inst.ExecInstance(&masterKey, `replace into bakufu_test.test_table (name, value) values ('TestMoveBelowAndBackComplex', ?)`, randValue)
	c.Assert(err, IsNil)
	master, err := inst.ReadTopologyInstance(&masterKey)
	c.Assert(err, IsNil)

	slave1, err = inst.MoveUp(&slave1Key)
	c.Assert(err, IsNil)
	_, err = inst.MasterPosWait(&slave1Key, &master.SelfBinlogCoordinates)
	c.Assert(err, IsNil)
	slave2, err := inst.ReadTopologyInstance(&slave2Key)
	c.Assert(err, IsNil)
	_, err = inst.MasterPosWait(&slave2Key, &master.SelfBinlogCoordinates)
	c.Assert(err, IsNil)
	var value1, value2 int
	inst.ScanInstanceRow(&slave1Key, `select value from bakufu_test.test_table where name='TestMoveBelowAndBackComplex'`, &value1)
	inst.ScanInstanceRow(&slave2Key, `select value from bakufu_test.test_table where name='TestMoveBelowAndBackComplex'`, &value2)

	c.Assert(inst.InstancesAreSiblings(slave1, slave2), Equals, true)
	c.Assert(value1, Equals, randValue)
	c.Assert(value2, Equals, randValue)
}

func (s *TestSuite) TestFailMoveBelow(c *C) {
	clearTestMaintenance()
	_, _ = inst.ExecInstance(&slave2Key, `set global binlog_format:='ROW'`)
	_, err := inst.MoveBelow(&slave1Key, &slave2Key)
	_, _ = inst.ExecInstance(&slave2Key, `set global binlog_format:='STATEMENT'`)
	c.Assert(err, Not(IsNil))
}

func (s *TestSuite) TestMakeCoMasterAndBack(c *C) {
	clearTestMaintenance()

	slave1, err := inst.MakeCoMaster(&slave1Key)
	c.Assert(err, IsNil)

	master, _ := inst.ReadTopologyInstance(&masterKey)
	c.Assert(master.IsSlaveOf(slave1), Equals, true)
	c.Assert(slave1.IsSlaveOf(master), Equals, true)

	master, err = inst.ResetSlaveOperation(&masterKey)
	slave1, _ = inst.ReadTopologyInstance(&slave1Key)
	c.Assert(err, IsNil)
	c.Assert(master.MasterKey.Hostname, Equals, "_")
}

func (s *TestSuite) TestFailMakeCoMaster(c *C) {
	clearTestMaintenance()
	_, err := inst.MakeCoMaster(&masterKey)
	c.Assert(err, Not(IsNil))
}

func (s *TestSuite) TestMakeCoMasterAndBackAndFailOthersToBecomeCoMasters(c *C) {
	clearTestMaintenance()

	slave1, err := inst.MakeCoMaster(&slave1Key)
	c.Assert(err, IsNil)

	master, _, _ := inst.ReadInstance(&masterKey)
	c.Assert(master.IsSlaveOf(slave1), Equals, true)
	c.Assert(slave1.IsSlaveOf(master), Equals, true)

	_, err = inst.MakeCoMaster(&masterKey)
	c.Assert(err, Not(IsNil))
	_, err = inst.MakeCoMaster(&slave1Key)
	c.Assert(err, Not(IsNil))
	_, err = inst.MakeCoMaster(&slave2Key)
	c.Assert(err, Not(IsNil))

	master, err = inst.ResetSlaveOperation(&masterKey)
	c.Assert(err, IsNil)
	c.Assert(master.MasterKey.Hostname, Equals, "_")
}

func (s *TestSuite) TestDiscover(c *C) {
	var err error
	_, err = db.ExecBakufu("delete from database_instance where hostname = ? and port = ?", masterKey.Hostname, masterKey.Port)
	_, err = db.ExecBakufu("delete from database_instance where hostname = ? and port = ?", slave1Key.Hostname, slave1Key.Port)
	_, err = db.ExecBakufu("delete from database_instance where hostname = ? and port = ?", slave2Key.Hostname, slave2Key.Port)
	_, err = db.ExecBakufu("delete from database_instance where hostname = ? and port = ?", slave3Key.Hostname, slave3Key.Port)
	_, found, _ := inst.ReadInstance(&masterKey)
	c.Assert(found, Equals, false)
	_, _ = inst.ReadTopologyInstance(&slave1Key)
	logic.StartDiscovery(slave1Key)
	_, found, err = inst.ReadInstance(&slave1Key)
	c.Assert(found, Equals, true)
	c.Assert(err, IsNil)
}

func (s *TestSuite) TestForgetMaster(c *C) {
	_, _ = inst.ReadTopologyInstance(&masterKey)
	_, found, _ := inst.ReadInstance(&masterKey)
	c.Assert(found, Equals, true)
	inst.ForgetInstance(&masterKey)
	_, found, _ = inst.ReadInstance(&masterKey)
	c.Assert(found, Equals, false)
}

func (s *TestSuite) TestCluster(c *C) {
	inst.ReadInstance(&masterKey)
	logic.StartDiscovery(slave1Key)
	instances, _ := inst.ReadClusterInstances(fmt.Sprintf("%s:%d", masterKey.Hostname, masterKey.Port))
	c.Assert(len(instances) >= 1, Equals, true)
}

func (s *TestSuite) TestBeginMaintenance(c *C) {
	clearTestMaintenance()
	_, _ = inst.ReadTopologyInstance(&masterKey)
	_, err := inst.BeginMaintenance(&masterKey, "unittest", "TestBeginMaintenance")

	c.Assert(err, IsNil)
}

func (s *TestSuite) TestBeginEndMaintenance(c *C) {
	clearTestMaintenance()
	_, _ = inst.ReadTopologyInstance(&masterKey)
	k, err := inst.BeginMaintenance(&masterKey, "unittest", "TestBeginEndMaintenance")
	c.Assert(err, IsNil)
	err = inst.EndMaintenance(k)
	c.Assert(err, IsNil)
}

func (s *TestSuite) TestFailBeginMaintenanceTwice(c *C) {
	clearTestMaintenance()
	_, _ = inst.ReadTopologyInstance(&masterKey)
	_, err := inst.BeginMaintenance(&masterKey, "unittest", "TestFailBeginMaintenanceTwice")
	c.Assert(err, IsNil)
	_, err = inst.BeginMaintenance(&masterKey, "unittest", "TestFailBeginMaintenanceTwice")
	c.Assert(err, Not(IsNil))
}

func (s *TestSuite) TestFailEndMaintenanceTwice(c *C) {
	clearTestMaintenance()
	_, _ = inst.ReadTopologyInstance(&masterKey)
	k, err := inst.BeginMaintenance(&masterKey, "unittest", "TestFailEndMaintenanceTwice")
	c.Assert(err, IsNil)
	err = inst.EndMaintenance(k)
	c.Assert(err, IsNil)
	err = inst.EndMaintenance(k)
	c.Assert(err, Not(IsNil))
}

func (s *TestSuite) TestFailMoveBelowUponMaintenance(c *C) {
	clearTestMaintenance()
	_, _ = inst.ReadTopologyInstance(&slave1Key)
	k, err := inst.BeginMaintenance(&slave1Key, "unittest", "TestBeginEndMaintenance")
	c.Assert(err, IsNil)

	_, err = inst.MoveBelow(&slave1Key, &slave2Key)
	c.Assert(err, Not(IsNil))

	err = inst.EndMaintenance(k)
	c.Assert(err, IsNil)
}

func (s *TestSuite) TestFailMoveBelowUponSlaveStopped(c *C) {
	clearTestMaintenance()

	slave1, _ := inst.ReadTopologyInstance(&slave1Key)
	c.Assert(slave1.SlaveRunning(), Equals, true)
	slave1, _ = inst.StopSlaveNicely(&slave1.Key, 0)
	c.Assert(slave1.SlaveRunning(), Equals, false)

	_, err := inst.MoveBelow(&slave1Key, &slave2Key)
	c.Assert(err, Not(IsNil))

	_, _ = inst.StartSlave(&slave1.Key)
}

func (s *TestSuite) TestFailMoveBelowUponOtherSlaveStopped(c *C) {
	clearTestMaintenance()

	slave1, _ := inst.ReadTopologyInstance(&slave1Key)
	c.Assert(slave1.SlaveRunning(), Equals, true)
	slave1, _ = inst.StopSlaveNicely(&slave1.Key, 0)
	c.Assert(slave1.SlaveRunning(), Equals, false)

	_, err := inst.MoveBelow(&slave2Key, &slave1Key)
	c.Assert(err, Not(IsNil))

	_, _ = inst.StartSlave(&slave1.Key)
}
