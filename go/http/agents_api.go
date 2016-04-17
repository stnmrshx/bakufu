/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package http

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-martini/martini"
	"github.com/martini-contrib/render"

	"github.com/stnmrshx/bakufu/go/agent"
	"github.com/stnmrshx/bakufu/go/attributes"
)

type HttpAgentsAPI struct{}

var AgentsAPI HttpAgentsAPI = HttpAgentsAPI{}

func (this *HttpAgentsAPI) SubmitAgent(params martini.Params, r render.Render) {
	port, err := strconv.Atoi(params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	output, err := agent.SubmitAgent(params["host"], port, params["token"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	r.JSON(200, output)
}

func (this *HttpAgentsAPI) SetHostAttribute(params martini.Params, r render.Render, req *http.Request) {
	err := attributes.SetHostAttributes(params["host"], params["attrVame"], params["attrValue"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, (err == nil))
}

func (this *HttpAgentsAPI) GetHostAttributeByAttributeName(params martini.Params, r render.Render, req *http.Request) {

	output, err := attributes.GetHostAttributesByAttribute(params["attr"], req.URL.Query().Get("valueMatch"))

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, output)
}

func (this *HttpAgentsAPI) AgentsHosts(params martini.Params, r render.Render, req *http.Request) string {
	agents, err := agent.ReadAgents()
	hostnames := []string{}
	for _, agent := range agents {
		hostnames = append(hostnames, agent.Hostname)
	}

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return ""
	}

	if req.URL.Query().Get("format") == "txt" {
		return strings.Join(hostnames, "\n")
	} else {
		r.JSON(200, hostnames)
	}
	return ""
}

func (this *HttpAgentsAPI) AgentsInstances(params martini.Params, r render.Render, req *http.Request) string {
	agents, err := agent.ReadAgents()
	hostnames := []string{}
	for _, agent := range agents {
		hostnames = append(hostnames, fmt.Sprintf("%s:%d", agent.Hostname, agent.MySQLPort))
	}

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return ""
	}

	if req.URL.Query().Get("format") == "txt" {
		return strings.Join(hostnames, "\n")
	} else {
		r.JSON(200, hostnames)
	}
	return ""
}

func (this *HttpAgentsAPI) AgentPing(params martini.Params, r render.Render, req *http.Request) {
	r.JSON(200, "OK")
}

func (this *HttpAgentsAPI) RegisterRequests(m *martini.ClassicMartini) {
	m.Get("/api/submit-agent/:host/:port/:token", this.SubmitAgent)
	m.Get("/api/host-attribute/:host/:attrVame/:attrValue", this.SetHostAttribute)
	m.Get("/api/host-attribute/attr/:attr/", this.GetHostAttributeByAttributeName)
	m.Get("/api/agents-hosts", this.AgentsHosts)
	m.Get("/api/agents-instances", this.AgentsInstances)
	m.Get("/api/agent-ping", this.AgentPing)
}
