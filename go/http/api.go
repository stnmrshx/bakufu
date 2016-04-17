/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package http

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"

	"github.com/go-martini/martini"
	"github.com/martini-contrib/auth"
	"github.com/martini-contrib/render"

	"github.com/stnmrshx/golib/util"

	"github.com/stnmrshx/bakufu/go/agent"
	"github.com/stnmrshx/bakufu/go/config"
	"github.com/stnmrshx/bakufu/go/inst"
	"github.com/stnmrshx/bakufu/go/logic"
	"github.com/stnmrshx/bakufu/go/process"
)

type APIResponseCode int

const (
	ERROR APIResponseCode = iota
	OK
)

func (this *APIResponseCode) MarshalJSON() ([]byte, error) {
	return json.Marshal(this.String())
}

func (this *APIResponseCode) String() string {
	switch *this {
	case ERROR:
		return "ERROR"
	case OK:
		return "OK"
	}
	return "unknown"
}

type APIResponse struct {
	Code    APIResponseCode
	Message string
	Details interface{}
}

type HttpAPI struct{}

var API HttpAPI = HttpAPI{}

func (this *HttpAPI) getInstanceKey(host string, port string) (inst.InstanceKey, error) {
	instanceKey, err := inst.NewInstanceKeyFromStrings(host, port)
	return *instanceKey, err
}

func (this *HttpAPI) getBinlogCoordinates(logFile string, logPos string) (inst.BinlogCoordinates, error) {
	coordinates := inst.BinlogCoordinates{LogFile: logFile}
	var err error
	if coordinates.LogPos, err = strconv.ParseInt(logPos, 10, 0); err != nil {
		return coordinates, fmt.Errorf("Invalid logPos: %s", logPos)
	}

	return coordinates, err
}

func (this *HttpAPI) Instance(params martini.Params, r render.Render, req *http.Request) {
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, found, err := inst.ReadInstance(&instanceKey)
	if (!found) || (err != nil) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Cannot read instance: %+v", instanceKey)})
		return
	}
	r.JSON(200, instance)
}

func (this *HttpAPI) Discover(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.ReadTopologyInstance(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance discovered: %+v", instance.Key), Details: instance})
}

func (this *HttpAPI) Refresh(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	_, err = inst.RefreshTopologyInstance(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance refreshed: %+v", instanceKey), Details: instanceKey})
}

func (this *HttpAPI) Forget(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	rawInstanceKey, _ := inst.NewRawInstanceKey(fmt.Sprintf("%s:%s", params["host"], params["port"]))

	inst.ForgetInstance(rawInstanceKey)

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance forgotten: %+v", *rawInstanceKey)})
}

func (this *HttpAPI) Resolve(params martini.Params, r render.Render, req *http.Request) {
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	if conn, err := net.Dial("tcp", instanceKey.DisplayString()); err == nil {
		conn.Close()
	} else {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: "Instance resolved", Details: instanceKey})
}

func (this *HttpAPI) BeginMaintenance(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	key, err := inst.BeginMaintenance(&instanceKey, params["owner"], params["reason"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error(), Details: key})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Maintenance begun: %+v", instanceKey)})
}

func (this *HttpAPI) EndMaintenance(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	maintenanceKey, err := strconv.ParseInt(params["maintenanceKey"], 10, 0)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	err = inst.EndMaintenance(maintenanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Maintenance ended: %+v", maintenanceKey)})
}

func (this *HttpAPI) EndMaintenanceByInstanceKey(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	err = inst.EndMaintenanceByInstanceKey(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Maintenance ended: %+v", instanceKey)})
}

func (this *HttpAPI) Maintenance(params martini.Params, r render.Render, req *http.Request) {
	instanceKeys, err := inst.ReadActiveMaintenance()

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, instanceKeys)
}

func (this *HttpAPI) BeginDowntime(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	var durationSeconds int = 0
	if params["duration"] != "" {
		durationSeconds, err = util.SimpleTimeToSeconds(params["duration"])
		if durationSeconds < 0 {
			err = fmt.Errorf("Duration value must be non-negative. Given value: %d", durationSeconds)
		}
		if err != nil {
			r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
			return
		}
	}

	err = inst.BeginDowntime(&instanceKey, params["owner"], params["reason"], uint(durationSeconds))

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error(), Details: instanceKey})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Downtime begun: %+v", instanceKey)})
}

func (this *HttpAPI) EndDowntime(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	err = inst.EndDowntime(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Downtime ended: %+v", instanceKey)})
}

func (this *HttpAPI) MoveUp(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.MoveUp(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance %+v moved up", instanceKey), Details: instance})
}

func (this *HttpAPI) MoveUpSlaves(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	slaves, newMaster, err, errs := inst.MoveUpSlaves(&instanceKey, req.URL.Query().Get("pattern"))
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Moved up %d slaves of %+v below %+v; %d errors: %+v", len(slaves), instanceKey, newMaster.Key, len(errs), errs), Details: newMaster.Key})
}

func (this *HttpAPI) RepointSlaves(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	slaves, err, _ := inst.RepointSlaves(&instanceKey, req.URL.Query().Get("pattern"))
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Repointed %d slaves of %+v", len(slaves), instanceKey), Details: instanceKey})
}

func (this *HttpAPI) MakeCoMaster(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.MakeCoMaster(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance made co-master: %+v", instance.Key), Details: instance})
}

func (this *HttpAPI) ResetSlave(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.ResetSlaveOperation(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Slave reset on %+v", instance.Key), Details: instance})
}

func (this *HttpAPI) DetachSlave(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.DetachSlaveOperation(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Slave detached: %+v", instance.Key), Details: instance})
}

func (this *HttpAPI) ReattachSlave(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.ReattachSlaveOperation(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Slave reattached: %+v", instance.Key), Details: instance})
}

func (this *HttpAPI) EnableGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.EnableGTID(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Enabled GTID on %+v", instance.Key), Details: instance})
}

func (this *HttpAPI) DisableGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.DisableGTID(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Disabled GTID on %+v", instance.Key), Details: instance})
}

func (this *HttpAPI) MoveBelow(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	siblingKey, err := this.getInstanceKey(params["siblingHost"], params["siblingPort"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	instance, err := inst.MoveBelow(&instanceKey, &siblingKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance %+v moved below %+v", instanceKey, siblingKey), Details: instance})
}

func (this *HttpAPI) MoveBelowGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	belowKey, err := this.getInstanceKey(params["belowHost"], params["belowPort"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	instance, err := inst.MoveBelowGTID(&instanceKey, &belowKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance %+v moved below %+v via GTID", instanceKey, belowKey), Details: instance})
}

func (this *HttpAPI) MoveSlavesGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	belowKey, err := this.getInstanceKey(params["belowHost"], params["belowPort"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	movedSlaves, _, err, errs := inst.MoveSlavesGTID(&instanceKey, &belowKey, req.URL.Query().Get("pattern"))
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Moved %d slaves of %+v below %+v via GTID; %d errors: %+v", len(movedSlaves), instanceKey, belowKey, len(errs), errs), Details: belowKey})
}

func (this *HttpAPI) EnslaveSiblings(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	instance, count, err := inst.EnslaveSiblings(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Enslaved %d siblings of %+v", count, instanceKey), Details: instance})
}

func (this *HttpAPI) EnslaveMaster(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	instance, err := inst.EnslaveMaster(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("%+v enslaved its master", instanceKey), Details: instance})
}

func (this *HttpAPI) RelocateBelow(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	belowKey, err := this.getInstanceKey(params["belowHost"], params["belowPort"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	instance, err := inst.RelocateBelow(&instanceKey, &belowKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance %+v relocated below %+v", instanceKey, belowKey), Details: instance})
}

func (this *HttpAPI) RelocateSlaves(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	belowKey, err := this.getInstanceKey(params["belowHost"], params["belowPort"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	slaves, _, err, errs := inst.RelocateSlaves(&instanceKey, &belowKey, req.URL.Query().Get("pattern"))
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Relocated %d slaves of %+v below %+v; %d errors: %+v", len(slaves), instanceKey, belowKey, len(errs), errs), Details: slaves})
}

func (this *HttpAPI) MoveEquivalent(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	belowKey, err := this.getInstanceKey(params["belowHost"], params["belowPort"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	instance, err := inst.MoveEquivalent(&instanceKey, &belowKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance %+v relocated via equivalence coordinates below %+v", instanceKey, belowKey), Details: instance})
}

func (this *HttpAPI) LastPseudoGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	instance, err := inst.ReadTopologyInstance(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	if instance == nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Instance not found: %+v", instanceKey)})
		return
	}
	coordinates, text, err := inst.FindLastPseudoGTIDEntry(instance, instance.RelaylogCoordinates, nil, false, nil)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("%+v", *coordinates), Details: text})
}

func (this *HttpAPI) MatchBelow(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	belowKey, err := this.getInstanceKey(params["belowHost"], params["belowPort"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	instance, matchedCoordinates, err := inst.MatchBelow(&instanceKey, &belowKey, true)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance %+v matched below %+v at %+v", instanceKey, belowKey, *matchedCoordinates), Details: instance})
}

func (this *HttpAPI) MatchUp(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	instance, matchedCoordinates, err := inst.MatchUp(&instanceKey, true)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance %+v matched up at %+v", instanceKey, *matchedCoordinates), Details: instance})
}

func (this *HttpAPI) MultiMatchSlaves(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	belowKey, err := this.getInstanceKey(params["belowHost"], params["belowPort"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	slaves, newMaster, err, errs := inst.MultiMatchSlaves(&instanceKey, &belowKey, req.URL.Query().Get("pattern"))
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Matched %d slaves of %+v below %+v; %d errors: %+v", len(slaves), instanceKey, newMaster.Key, len(errs), errs), Details: newMaster.Key})
}

func (this *HttpAPI) MatchUpSlaves(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	slaves, newMaster, err, errs := inst.MatchUpSlaves(&instanceKey, req.URL.Query().Get("pattern"))
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Matched up %d slaves of %+v below %+v; %d errors: %+v", len(slaves), instanceKey, newMaster.Key, len(errs), errs), Details: newMaster.Key})
}

func (this *HttpAPI) RegroupSlaves(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	lostSlaves, equalSlaves, aheadSlaves, promotedSlave, err := inst.RegroupSlaves(&instanceKey, false, nil, nil)

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("promoted slave: %s, lost: %d, trivial: %d, pseudo-gtid: %d",
		promotedSlave.Key.DisplayString(), len(lostSlaves), len(equalSlaves), len(aheadSlaves)), Details: promotedSlave.Key})
}

func (this *HttpAPI) RegroupSlavesPseudoGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	lostSlaves, equalSlaves, aheadSlaves, promotedSlave, err := inst.RegroupSlavesPseudoGTID(&instanceKey, false, nil, nil)

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("promoted slave: %s, lost: %d, trivial: %d, pseudo-gtid: %d",
		promotedSlave.Key.DisplayString(), len(lostSlaves), len(equalSlaves), len(aheadSlaves)), Details: promotedSlave.Key})
}

func (this *HttpAPI) RegroupSlavesGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	lostSlaves, movedSlaves, promotedSlave, err := inst.RegroupSlavesGTID(&instanceKey, false, nil)

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("promoted slave: %s, lost: %d, moved: %d",
		promotedSlave.Key.DisplayString(), len(lostSlaves), len(movedSlaves)), Details: promotedSlave.Key})
}

func (this *HttpAPI) RegroupSlavesBinlogServers(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	_, promotedBinlogServer, err := inst.RegroupSlavesBinlogServers(&instanceKey, false)

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("promoted binlog server: %s",
		promotedBinlogServer.Key.DisplayString()), Details: promotedBinlogServer.Key})
}

func (this *HttpAPI) MakeMaster(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	instance, err := inst.MakeMaster(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance %+v now made master", instanceKey), Details: instance})
}

func (this *HttpAPI) MakeLocalMaster(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	instance, err := inst.MakeLocalMaster(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance %+v now made local master", instanceKey), Details: instance})
}

func (this *HttpAPI) SkipQuery(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.SkipQuery(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Query skipped on %+v", instance.Key), Details: instance})
}

func (this *HttpAPI) StartSlave(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.StartSlave(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Slave started: %+v", instance.Key), Details: instance})
}

func (this *HttpAPI) RestartSlave(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.RestartSlave(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Slave restarted: %+v", instance.Key), Details: instance})
}

func (this *HttpAPI) StopSlave(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.StopSlave(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Slave stopped: %+v", instance.Key), Details: instance})
}

func (this *HttpAPI) StopSlaveNicely(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.StopSlaveNicely(&instanceKey, 0)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Slave stopped nicely: %+v", instance.Key), Details: instance})
}

func (this *HttpAPI) MasterEquivalent(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	coordinates, err := this.getBinlogCoordinates(params["logFile"], params["logPos"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instanceCoordinates := &inst.InstanceBinlogCoordinates{Key: instanceKey, Coordinates: coordinates}

	equivalentCoordinates, err := inst.GetEquivalentMasterCoordinates(instanceCoordinates)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Found %+v equivalent coordinates", len(equivalentCoordinates)), Details: equivalentCoordinates})
}

func (this *HttpAPI) SetReadOnly(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.SetReadOnly(&instanceKey, true)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: "Server set as read-only", Details: instance})
}

func (this *HttpAPI) SetWriteable(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.SetReadOnly(&instanceKey, false)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: "Server set as writeable", Details: instance})
}

func (this *HttpAPI) KillQuery(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	processId, err := strconv.ParseInt(params["process"], 10, 0)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.KillQuery(&instanceKey, processId)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Query killed on : %+v", instance.Key), Details: instance})
}

func (this *HttpAPI) Cluster(params martini.Params, r render.Render, req *http.Request) {
	instances, err := inst.ReadClusterInstances(params["clusterName"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, instances)
}

func (this *HttpAPI) ClusterByAlias(params martini.Params, r render.Render, req *http.Request) {
	clusterName, err := inst.GetClusterByAlias(params["clusterAlias"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	params["clusterName"] = clusterName
	this.Cluster(params, r, req)
}

func (this *HttpAPI) ClusterByInstance(params martini.Params, r render.Render, req *http.Request) {
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, found, err := inst.ReadInstance(&instanceKey)
	if (!found) || (err != nil) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Cannot read instance: %+v", instanceKey)})
		return
	}

	params["clusterName"] = instance.ClusterName
	this.Cluster(params, r, req)
}

func (this *HttpAPI) ClusterInfo(params martini.Params, r render.Render, req *http.Request) {
	clusterInfo, err := inst.ReadClusterInfo(params["clusterName"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, clusterInfo)
}

func (this *HttpAPI) ClusterInfoByAlias(params martini.Params, r render.Render, req *http.Request) {
	clusterName, err := inst.GetClusterByAlias(params["clusterAlias"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	params["clusterName"] = clusterName
	this.ClusterInfo(params, r, req)
}

func (this *HttpAPI) ClusterOSCSlaves(params martini.Params, r render.Render, req *http.Request) {
	instances, err := inst.GetClusterOSCSlaves(params["clusterName"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, instances)
}

func (this *HttpAPI) SetClusterAlias(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	clusterName := params["clusterName"]
	alias := req.URL.Query().Get("alias")

	err := inst.SetClusterAlias(clusterName, alias)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}
	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Cluster %s now has alias '%s'", clusterName, alias)})
}

func (this *HttpAPI) Clusters(params martini.Params, r render.Render, req *http.Request) {
	clusterNames, err := inst.ReadClusters()

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, clusterNames)
}

func (this *HttpAPI) ClustersInfo(params martini.Params, r render.Render, req *http.Request) {
	clustersInfo, err := inst.ReadClustersInfo()

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, clustersInfo)
}

func (this *HttpAPI) Search(params martini.Params, r render.Render, req *http.Request) {
	searchString := params["searchString"]
	if searchString == "" {
		searchString = req.URL.Query().Get("s")
	}
	instances, err := inst.SearchInstances(searchString)

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, instances)
}

func (this *HttpAPI) Problems(params martini.Params, r render.Render, req *http.Request) {
	clusterName := params["clusterName"]
	instances, err := inst.ReadProblemInstances(clusterName)

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, instances)
}

func (this *HttpAPI) Audit(params martini.Params, r render.Render, req *http.Request) {
	page, err := strconv.Atoi(params["page"])
	if err != nil || page < 0 {
		page = 0
	}
	var auditedInstanceKey *inst.InstanceKey
	if instanceKey, err := this.getInstanceKey(params["host"], params["port"]); err == nil {
		auditedInstanceKey = &instanceKey
	}

	audits, err := inst.ReadRecentAudit(auditedInstanceKey, page)

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, audits)
}

func (this *HttpAPI) LongQueries(params martini.Params, r render.Render, req *http.Request) {
	longQueries, err := inst.ReadLongRunningProcesses(params["filter"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, longQueries)
}

func (this *HttpAPI) HostnameResolveCache(params martini.Params, r render.Render, req *http.Request) {
	content, err := inst.HostnameResolveCache()

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: "Cache retrieved", Details: content})
}

func (this *HttpAPI) ResetHostnameResolveCache(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	err := inst.ResetHostnameResolveCache()

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: "Hostname cache cleared"})
}

func (this *HttpAPI) SubmitPoolInstances(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	pool := params["pool"]
	instances := req.URL.Query().Get("instances")

	err := inst.ApplyPoolInstances(pool, instances)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Applied %s pool instances", pool)})
}

func (this *HttpAPI) ReadClusterPoolInstancesMap(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	clusterName := params["clusterName"]
	pool := params["pool"]

	poolInstancesMap, err := inst.ReadClusterPoolInstancesMap(clusterName, pool)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Read pool instances for cluster %s", clusterName), Details: poolInstancesMap})
}

func (this *HttpAPI) GetHeuristicClusterPoolInstances(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	clusterName, err := inst.ReadClusterByAlias(params["clusterName"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}
	pool := params["pool"]

	instances, err := inst.GetHeuristicClusterPoolInstances(clusterName, pool)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Heuristic pool instances for cluster %s", clusterName), Details: instances})
}

func (this *HttpAPI) GetHeuristicClusterPoolInstancesLag(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	clusterName, err := inst.ReadClusterByAlias(params["clusterName"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}
	pool := params["pool"]

	lag, err := inst.GetHeuristicClusterPoolInstancesLag(clusterName, pool)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Heuristic pool lag for cluster %s", clusterName), Details: lag})
}

func (this *HttpAPI) ReloadClusterAlias(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	err := inst.ReadClusterAliases()

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: "Cluster alias cache reloaded"})
}

func (this *HttpAPI) Agents(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	agents, err := agent.ReadAgents()

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, agents)
}

func (this *HttpAPI) Agent(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	agent, err := agent.GetAgent(params["host"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, agent)
}

func (this *HttpAPI) AgentUnmount(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	output, err := agent.Unmount(params["host"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, output)
}

func (this *HttpAPI) AgentMountLV(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	output, err := agent.MountLV(params["host"], req.URL.Query().Get("lv"))

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, output)
}

func (this *HttpAPI) AgentCreateSnapshot(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	output, err := agent.CreateSnapshot(params["host"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, output)
}

func (this *HttpAPI) AgentRemoveLV(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	output, err := agent.RemoveLV(params["host"], req.URL.Query().Get("lv"))

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, output)
}

func (this *HttpAPI) AgentMySQLStop(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	output, err := agent.MySQLStop(params["host"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, output)
}

func (this *HttpAPI) AgentMySQLStart(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	output, err := agent.MySQLStart(params["host"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, output)
}

func (this *HttpAPI) AgentSeed(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	output, err := agent.Seed(params["targetHost"], params["sourceHost"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, output)
}

func (this *HttpAPI) AgentActiveSeeds(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	output, err := agent.ReadActiveSeedsForHost(params["host"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, output)
}

func (this *HttpAPI) AgentRecentSeeds(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	output, err := agent.ReadRecentCompletedSeedsForHost(params["host"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, output)
}

func (this *HttpAPI) AgentSeedDetails(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	seedId, err := strconv.ParseInt(params["seedId"], 10, 0)
	output, err := agent.AgentSeedDetails(seedId)

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, output)
}

func (this *HttpAPI) AgentSeedStates(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	seedId, err := strconv.ParseInt(params["seedId"], 10, 0)
	output, err := agent.ReadSeedStates(seedId)

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, output)
}

func (this *HttpAPI) Seeds(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	output, err := agent.ReadRecentSeeds()

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, output)
}

func (this *HttpAPI) AbortSeed(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	seedId, err := strconv.ParseInt(params["seedId"], 10, 0)
	err = agent.AbortSeed(seedId)

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, err == nil)
}

func (this *HttpAPI) Headers(params martini.Params, r render.Render, req *http.Request) {
	r.JSON(200, req.Header)
}

func (this *HttpAPI) Health(params martini.Params, r render.Render, req *http.Request) {
	health, err := process.HealthTest()
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Application node is unhealthy %+v", err), Details: health})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Application node is healthy"), Details: health})

}

func (this *HttpAPI) LBCheck(params martini.Params, r render.Render, req *http.Request) {
	r.JSON(200, "OK")
}

func (this *HttpAPI) StatusCheck(params martini.Params, r render.Render, req *http.Request) {
	var health *process.HealthStatus
	var err error
	if config.Config.StatusSimpleHealth {
		health, err = process.SimpleHealthTest()
	} else {
		health, err = process.HealthTest()
	}
	if err != nil {
		r.JSON(500, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Application node is unhealthy %+v", err), Details: health})
		return
	}
	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Application node is healthy"), Details: health})
}

func (this *HttpAPI) GrabElection(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	err := process.GrabElection()
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Unable to grab election: %+v", err)})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Node elected as leader")})
}

func (this *HttpAPI) Reelect(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	err := process.Reelect()
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Unable to re-elect: %+v", err)})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Set re-elections")})

}

func (this *HttpAPI) ReloadConfiguration(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	config.Reload()
	inst.AuditOperation("reload-configuration", nil, "Triggered via API")

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Config reloaded")})

}

func (this *HttpAPI) ReplicationAnalysis(params martini.Params, r render.Render, req *http.Request) {
	analysis, err := inst.GetReplicationAnalysis(params["clusterName"], true, false)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Cannot get analysis: %+v", err)})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Analysis"), Details: analysis})
}

func (this *HttpAPI) RecoverLite(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	params["skipProcesses"] = "true"
	this.Recover(params, r, req, user)
}

func (this *HttpAPI) Recover(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	var candidateKey *inst.InstanceKey
	if key, err := this.getInstanceKey(params["candidateHost"], params["candidatePort"]); err == nil {
		candidateKey = &key
	}

	skipProcesses := (req.URL.Query().Get("skipProcesses") == "true") || (params["skipProcesses"] == "true")
	recoveryAttempted, _, err := logic.CheckAndRecover(&instanceKey, candidateKey, skipProcesses)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	if recoveryAttempted {
		r.JSON(200, &APIResponse{Code: OK, Message: "Action taken", Details: instanceKey})
	} else {
		r.JSON(200, &APIResponse{Code: OK, Message: "No action taken", Details: instanceKey})
	}
}

func (this *HttpAPI) RegisterCandidate(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	switch params["promotionRule"] {
	case "prefer", "neutral", "must_not":
		{
			// MMMKKAAAAYYY
		}
	default:
		{
			r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Invalid promotionRule: %+v", params["promotionRule"])})
			return
		}
	}

	promotionRule := inst.CandidatePromotionRule(params["promotionRule"])

	err = inst.RegisterCandidateInstance(&instanceKey, promotionRule)

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: "Registered candidate", Details: instanceKey})
}

func (this *HttpAPI) AutomatedRecoveryFilters(params martini.Params, r render.Render, req *http.Request) {
	automatedRecoveryMap := make(map[string]interface{})
	automatedRecoveryMap["RecoverMasterClusterFilters"] = config.Config.RecoverMasterClusterFilters
	automatedRecoveryMap["RecoverIntermediateMasterClusterFilters"] = config.Config.RecoverIntermediateMasterClusterFilters
	automatedRecoveryMap["RecoveryIgnoreHostnameFilters"] = config.Config.RecoveryIgnoreHostnameFilters

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Automated recovery configuration details"), Details: automatedRecoveryMap})
}

func (this *HttpAPI) AuditFailureDetection(params martini.Params, r render.Render, req *http.Request) {

	var audits []logic.TopologyRecovery
	var err error

	if detectionId, derr := strconv.ParseInt(params["id"], 10, 0); derr == nil && detectionId > 0 {
		audits, err = logic.ReadFailureDetection(detectionId)
	} else {
		page, derr := strconv.Atoi(params["page"])
		if derr != nil || page < 0 {
			page = 0
		}
		audits, err = logic.ReadRecentFailureDetections(page)
	}

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, audits)
}

func (this *HttpAPI) ReadReplicationAnalysisChangelog(params martini.Params, r render.Render, req *http.Request) {
	changelogs, err := inst.ReadReplicationAnalysisChangelog()

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, changelogs)
}

func (this *HttpAPI) AuditRecovery(params martini.Params, r render.Render, req *http.Request) {
	var audits []logic.TopologyRecovery
	var err error

	if recoveryId, derr := strconv.ParseInt(params["id"], 10, 0); derr == nil && recoveryId > 0 {
		audits, err = logic.ReadRecovery(recoveryId)
	} else {
		page, derr := strconv.Atoi(params["page"])
		if derr != nil || page < 0 {
			page = 0
		}
		unacknowledgedOnly := (req.URL.Query().Get("unacknowledged") == "true")
		audits, err = logic.ReadRecentRecoveries(params["clusterName"], unacknowledgedOnly, page)
	}

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, audits)
}

func (this *HttpAPI) ActiveClusterRecovery(params martini.Params, r render.Render, req *http.Request) {
	recoveries, err := logic.ReadActiveClusterRecovery(params["clusterName"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, recoveries)
}

func (this *HttpAPI) RecentlyActiveClusterRecovery(params martini.Params, r render.Render, req *http.Request) {
	recoveries, err := logic.ReadRecentlyActiveClusterRecovery(params["clusterName"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, recoveries)
}

func (this *HttpAPI) RecentlyActiveInstanceRecovery(params martini.Params, r render.Render, req *http.Request) {
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	recoveries, err := logic.ReadRecentlyActiveInstanceRecovery(&instanceKey)

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, recoveries)
}

func (this *HttpAPI) AcknowledgeClusterRecoveries(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}

	clusterName := params["clusterName"]
	if params["clusterAlias"] != "" {
		var err error
		clusterName, err = inst.GetClusterByAlias(params["clusterAlias"])
		if err != nil {
			r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
			return
		}
	}

	comment := req.URL.Query().Get("comment")
	if comment == "" {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("No acknowledge comment given")})
		return
	}
	userId := getUserId(req, user)
	if userId == "" {
		userId = inst.GetMaintenanceOwner()
	}
	countAcnowledgedRecoveries, err := logic.AcknowledgeClusterRecoveries(clusterName, userId, comment)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, countAcnowledgedRecoveries)
}

func (this *HttpAPI) AcknowledgeInstanceRecoveries(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}

	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	comment := req.URL.Query().Get("comment")
	if comment == "" {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("No acknowledge comment given")})
		return
	}
	userId := getUserId(req, user)
	if userId == "" {
		userId = inst.GetMaintenanceOwner()
	}
	countAcnowledgedRecoveries, err := logic.AcknowledgeInstanceRecoveries(&instanceKey, userId, comment)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, countAcnowledgedRecoveries)
}

func (this *HttpAPI) AcknowledgeRecovery(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}

	recoveryId, err := strconv.ParseInt(params["recoveryId"], 10, 0)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	comment := req.URL.Query().Get("comment")
	if comment == "" {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("No acknowledge comment given")})
		return
	}
	userId := getUserId(req, user)
	if userId == "" {
		userId = inst.GetMaintenanceOwner()
	}
	countAcnowledgedRecoveries, err := logic.AcknowledgeRecovery(recoveryId, userId, comment)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, countAcnowledgedRecoveries)
}

func (this *HttpAPI) BlockedRecoveries(params martini.Params, r render.Render, req *http.Request) {
	blockedRecoveries, err := logic.ReadBlockedRecoveries(params["clusterName"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, blockedRecoveries)
}

func (this *HttpAPI) RegisterRequests(m *martini.ClassicMartini) {
	m.Get("/api/relocate/:host/:port/:belowHost/:belowPort", this.RelocateBelow)
	m.Get("/api/relocate-below/:host/:port/:belowHost/:belowPort", this.RelocateBelow)
	m.Get("/api/relocate-slaves/:host/:port/:belowHost/:belowPort", this.RelocateSlaves)
	m.Get("/api/regroup-slaves/:host/:port", this.RegroupSlaves)
	m.Get("/api/move-up/:host/:port", this.MoveUp)
	m.Get("/api/move-up-slaves/:host/:port", this.MoveUpSlaves)
	m.Get("/api/move-below/:host/:port/:siblingHost/:siblingPort", this.MoveBelow)
	m.Get("/api/move-equivalent/:host/:port/:belowHost/:belowPort", this.MoveEquivalent)
	m.Get("/api/repoint-slaves/:host/:port", this.RepointSlaves)
	m.Get("/api/make-co-master/:host/:port", this.MakeCoMaster)
	m.Get("/api/enslave-siblings/:host/:port", this.EnslaveSiblings)
	m.Get("/api/enslave-master/:host/:port", this.EnslaveMaster)
	m.Get("/api/master-equivalent/:host/:port/:logFile/:logPos", this.MasterEquivalent)
	m.Get("/api/regroup-slaves-bls/:host/:port", this.RegroupSlavesBinlogServers)
	m.Get("/api/move-below-gtid/:host/:port/:belowHost/:belowPort", this.MoveBelowGTID)
	m.Get("/api/move-slaves-gtid/:host/:port/:belowHost/:belowPort", this.MoveSlavesGTID)
	m.Get("/api/regroup-slaves-gtid/:host/:port", this.RegroupSlavesGTID)
	m.Get("/api/match/:host/:port/:belowHost/:belowPort", this.MatchBelow)
	m.Get("/api/match-below/:host/:port/:belowHost/:belowPort", this.MatchBelow)
	m.Get("/api/match-up/:host/:port", this.MatchUp)
	m.Get("/api/match-slaves/:host/:port/:belowHost/:belowPort", this.MultiMatchSlaves)
	m.Get("/api/multi-match-slaves/:host/:port/:belowHost/:belowPort", this.MultiMatchSlaves)
	m.Get("/api/match-up-slaves/:host/:port", this.MatchUpSlaves)
	m.Get("/api/regroup-slaves-pgtid/:host/:port", this.RegroupSlavesPseudoGTID)
	m.Get("/api/make-master/:host/:port", this.MakeMaster)
	m.Get("/api/make-local-master/:host/:port", this.MakeLocalMaster)
	m.Get("/api/enable-gtid/:host/:port", this.EnableGTID)
	m.Get("/api/disable-gtid/:host/:port", this.DisableGTID)
	m.Get("/api/skip-query/:host/:port", this.SkipQuery)
	m.Get("/api/start-slave/:host/:port", this.StartSlave)
	m.Get("/api/restart-slave/:host/:port", this.RestartSlave)
	m.Get("/api/stop-slave/:host/:port", this.StopSlave)
	m.Get("/api/stop-slave-nice/:host/:port", this.StopSlaveNicely)
	m.Get("/api/reset-slave/:host/:port", this.ResetSlave)
	m.Get("/api/detach-slave/:host/:port", this.DetachSlave)
	m.Get("/api/reattach-slave/:host/:port", this.ReattachSlave)
	m.Get("/api/set-read-only/:host/:port", this.SetReadOnly)
	m.Get("/api/set-writeable/:host/:port", this.SetWriteable)
	m.Get("/api/kill-query/:host/:port/:process", this.KillQuery)
	m.Get("/api/last-pseudo-gtid/:host/:port", this.LastPseudoGTID)
	m.Get("/api/submit-pool-instances/:pool", this.SubmitPoolInstances)
	m.Get("/api/cluster-pool-instances/:clusterName", this.ReadClusterPoolInstancesMap)
	m.Get("/api/cluster-pool-instances/:clusterName/:pool", this.ReadClusterPoolInstancesMap)
	m.Get("/api/heuristic-cluster-pool-instances/:clusterName", this.GetHeuristicClusterPoolInstances)
	m.Get("/api/heuristic-cluster-pool-instances/:clusterName/:pool", this.GetHeuristicClusterPoolInstances)
	m.Get("/api/heuristic-cluster-pool-lag/:clusterName", this.GetHeuristicClusterPoolInstancesLag)
	m.Get("/api/heuristic-cluster-pool-lag/:clusterName/:pool", this.GetHeuristicClusterPoolInstancesLag)
	m.Get("/api/search/:searchString", this.Search)
	m.Get("/api/search", this.Search)
	m.Get("/api/cluster/:clusterName", this.Cluster)
	m.Get("/api/cluster/alias/:clusterAlias", this.ClusterByAlias)
	m.Get("/api/cluster/instance/:host/:port", this.ClusterByInstance)
	m.Get("/api/cluster-info/:clusterName", this.ClusterInfo)
	m.Get("/api/cluster-info/alias/:clusterAlias", this.ClusterInfoByAlias)
	m.Get("/api/cluster-osc-slaves/:clusterName", this.ClusterOSCSlaves)
	m.Get("/api/set-cluster-alias/:clusterName", this.SetClusterAlias)
	m.Get("/api/clusters", this.Clusters)
	m.Get("/api/clusters-info", this.ClustersInfo)
	m.Get("/api/instance/:host/:port", this.Instance)
	m.Get("/api/discover/:host/:port", this.Discover)
	m.Get("/api/refresh/:host/:port", this.Refresh)
	m.Get("/api/forget/:host/:port", this.Forget)
	m.Get("/api/begin-maintenance/:host/:port/:owner/:reason", this.BeginMaintenance)
	m.Get("/api/end-maintenance/:host/:port", this.EndMaintenanceByInstanceKey)
	m.Get("/api/end-maintenance/:maintenanceKey", this.EndMaintenance)
	m.Get("/api/begin-downtime/:host/:port/:owner/:reason", this.BeginDowntime)
	m.Get("/api/begin-downtime/:host/:port/:owner/:reason/:duration", this.BeginDowntime)
	m.Get("/api/end-downtime/:host/:port", this.EndDowntime)
	m.Get("/api/replication-analysis", this.ReplicationAnalysis)
	m.Get("/api/replication-analysis/:clusterName", this.ReplicationAnalysis)
	m.Get("/api/recover/:host/:port", this.Recover)
	m.Get("/api/recover/:host/:port/:candidateHost/:candidatePort", this.Recover)
	m.Get("/api/recover-lite/:host/:port", this.RecoverLite)
	m.Get("/api/recover-lite/:host/:port/:candidateHost/:candidatePort", this.RecoverLite)
	m.Get("/api/register-candidate/:host/:port/:promotionRule", this.RegisterCandidate)
	m.Get("/api/automated-recovery-filters", this.AutomatedRecoveryFilters)
	m.Get("/api/audit-failure-detection", this.AuditFailureDetection)
	m.Get("/api/audit-failure-detection/:page", this.AuditFailureDetection)
	m.Get("/api/audit-failure-detection/id/:id", this.AuditFailureDetection)
	m.Get("/api/replication-analysis-changelog", this.ReadReplicationAnalysisChangelog)
	m.Get("/api/audit-recovery", this.AuditRecovery)
	m.Get("/api/audit-recovery/:page", this.AuditRecovery)
	m.Get("/api/audit-recovery/id/:id", this.AuditRecovery)
	m.Get("/api/audit-recovery/cluster/:clusterName", this.AuditRecovery)
	m.Get("/api/audit-recovery/cluster/:clusterName/:page", this.AuditRecovery)
	m.Get("/api/active-cluster-recovery/:clusterName", this.ActiveClusterRecovery)
	m.Get("/api/recently-active-cluster-recovery/:clusterName", this.RecentlyActiveClusterRecovery)
	m.Get("/api/recently-active-instance-recovery/:host/:port", this.RecentlyActiveInstanceRecovery)
	m.Get("/api/ack-recovery/cluster/:clusterName", this.AcknowledgeClusterRecoveries)
	m.Get("/api/ack-recovery/cluster/alias/:clusterAlias", this.AcknowledgeClusterRecoveries)
	m.Get("/api/ack-recovery/instance/:host/:port", this.AcknowledgeInstanceRecoveries)
	m.Get("/api/ack-recovery/:recoveryId", this.AcknowledgeRecovery)
	m.Get("/api/blocked-recoveries", this.BlockedRecoveries)
	m.Get("/api/blocked-recoveries/cluster/:clusterName", this.BlockedRecoveries)
	m.Get("/api/problems", this.Problems)
	m.Get("/api/problems/:clusterName", this.Problems)
	m.Get("/api/long-queries", this.LongQueries)
	m.Get("/api/long-queries/:filter", this.LongQueries)
	m.Get("/api/audit", this.Audit)
	m.Get("/api/audit/:page", this.Audit)
	m.Get("/api/audit/instance/:host/:port", this.Audit)
	m.Get("/api/audit/instance/:host/:port/:page", this.Audit)
	m.Get("/api/resolve/:host/:port", this.Resolve)
	m.Get("/api/maintenance", this.Maintenance)
	m.Get("/api/headers", this.Headers)
	m.Get("/api/health", this.Health)
	m.Get("/api/lb-check", this.LBCheck)
	m.Get("/api/grab-election", this.GrabElection)
	m.Get("/api/reelect", this.Reelect)
	m.Get("/api/reload-configuration", this.ReloadConfiguration)
	m.Get("/api/reload-cluster-alias", this.ReloadClusterAlias)
	m.Get("/api/hostname-resolve-cache", this.HostnameResolveCache)
	m.Get("/api/reset-hostname-resolve-cache", this.ResetHostnameResolveCache)
	m.Get("/api/agents", this.Agents)
	m.Get("/api/agent/:host", this.Agent)
	m.Get("/api/agent-umount/:host", this.AgentUnmount)
	m.Get("/api/agent-mount/:host", this.AgentMountLV)
	m.Get("/api/agent-create-snapshot/:host", this.AgentCreateSnapshot)
	m.Get("/api/agent-removelv/:host", this.AgentRemoveLV)
	m.Get("/api/agent-mysql-stop/:host", this.AgentMySQLStop)
	m.Get("/api/agent-mysql-start/:host", this.AgentMySQLStart)
	m.Get("/api/agent-seed/:targetHost/:sourceHost", this.AgentSeed)
	m.Get("/api/agent-active-seeds/:host", this.AgentActiveSeeds)
	m.Get("/api/agent-recent-seeds/:host", this.AgentRecentSeeds)
	m.Get("/api/agent-seed-details/:seedId", this.AgentSeedDetails)
	m.Get("/api/agent-seed-states/:seedId", this.AgentSeedStates)
	m.Get("/api/agent-abort-seed/:seedId", this.AbortSeed)
	m.Get("/api/seeds", this.Seeds)
	m.Get(config.Config.StatusEndpoint, this.StatusCheck)
}
