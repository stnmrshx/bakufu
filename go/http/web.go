/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package http

import (
	"expvar"
	"fmt"
	"net/http"
	"net/http/pprof"
	"strconv"

	"github.com/go-martini/martini"
	"github.com/martini-contrib/auth"
	"github.com/martini-contrib/render"

	"github.com/stnmrshx/bakufu/go/config"
	"github.com/stnmrshx/bakufu/go/inst"
)

type HttpWeb struct{}

var Web HttpWeb = HttpWeb{}

func (this *HttpWeb) getInstanceKey(host string, port string) (inst.InstanceKey, error) {
	instanceKey := inst.InstanceKey{Hostname: host}
	var err error

	if instanceKey.Port, err = strconv.Atoi(port); err != nil {
		return instanceKey, fmt.Errorf("Invalid port: %s", port)
	}
	return instanceKey, err
}

func (this *HttpWeb) AccessToken(params martini.Params, r render.Render, req *http.Request, resp http.ResponseWriter, user auth.User) {
	publicToken := req.URL.Query().Get("publicToken")
	err := authenticateToken(publicToken, resp)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}
	r.Redirect("/")
}

func (this *HttpWeb) Clusters(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	r.HTML(200, "templates/clusters", map[string]interface{}{
		"agentsHttpActive":              config.Config.ServeAgentsHttp,
		"title":                         "clusters",
		"activePage":                    "cluster",
		"autoshow_problems":             false,
		"authorizedForAction":           isAuthorizedForAction(req, user),
		"userId":                        getUserId(req, user),
		"removeTextFromHostnameDisplay": config.Config.RemoveTextFromHostnameDisplay,
	})
}

func (this *HttpWeb) ClustersAnalysis(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	r.HTML(200, "templates/clusters_analysis", map[string]interface{}{
		"agentsHttpActive":              config.Config.ServeAgentsHttp,
		"title":                         "clusters",
		"activePage":                    "cluster",
		"autoshow_problems":             false,
		"authorizedForAction":           isAuthorizedForAction(req, user),
		"userId":                        getUserId(req, user),
		"removeTextFromHostnameDisplay": config.Config.RemoveTextFromHostnameDisplay,
	})
}

func (this *HttpWeb) Cluster(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	r.HTML(200, "templates/cluster", map[string]interface{}{
		"agentsHttpActive":              config.Config.ServeAgentsHttp,
		"title":                         "cluster",
		"activePage":                    "cluster",
		"clusterName":                   params["clusterName"],
		"autoshow_problems":             true,
		"contextMenuVisible":            true,
		"pseudoGTIDModeEnabled":         (config.Config.PseudoGTIDPattern != ""),
		"authorizedForAction":           isAuthorizedForAction(req, user),
		"userId":                        getUserId(req, user),
		"removeTextFromHostnameDisplay": config.Config.RemoveTextFromHostnameDisplay,
		"compactDisplay":                req.URL.Query().Get("compact"),
	})
}

func (this *HttpWeb) ClusterByAlias(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	clusterName, err := inst.GetClusterByAlias(params["clusterAlias"])
	if clusterName == "" && err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	params["clusterName"] = clusterName
	this.Cluster(params, r, req, user)
}

func (this *HttpWeb) ClusterByInstance(params martini.Params, r render.Render, req *http.Request, user auth.User) {
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

	if instance.ClusterName == "" && err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	params["clusterName"] = instance.ClusterName
	this.Cluster(params, r, req, user)
}

func (this *HttpWeb) ClusterPools(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	r.HTML(200, "templates/cluster_pools", map[string]interface{}{
		"agentsHttpActive":              config.Config.ServeAgentsHttp,
		"title":                         "cluster pools",
		"activePage":                    "cluster_pools",
		"clusterName":                   params["clusterName"],
		"autoshow_problems":             false,
		"contextMenuVisible":            true,
		"pseudoGTIDModeEnabled":         (config.Config.PseudoGTIDPattern != ""),
		"authorizedForAction":           isAuthorizedForAction(req, user),
		"userId":                        getUserId(req, user),
		"removeTextFromHostnameDisplay": config.Config.RemoveTextFromHostnameDisplay,
		"compactDisplay":                req.URL.Query().Get("compact"),
	})
}

func (this *HttpWeb) Search(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	searchString := params["searchString"]
	if searchString == "" {
		searchString = req.URL.Query().Get("s")
	}
	r.HTML(200, "templates/search", map[string]interface{}{
		"agentsHttpActive":    config.Config.ServeAgentsHttp,
		"title":               "search",
		"activePage":          "search",
		"searchString":        searchString,
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserId(req, user),
		"autoshow_problems":   false,
	})
}

func (this *HttpWeb) Discover(params martini.Params, r render.Render, req *http.Request, user auth.User) {

	r.HTML(200, "templates/discover", map[string]interface{}{
		"agentsHttpActive":    config.Config.ServeAgentsHttp,
		"title":               "discover",
		"activePage":          "discover",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserId(req, user),
		"autoshow_problems":   false,
	})
}

func (this *HttpWeb) LongQueries(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	filter := params["filter"]
	if filter == "" {
		filter = req.URL.Query().Get("filter")
	}

	r.HTML(200, "templates/long_queries", map[string]interface{}{
		"agentsHttpActive":    config.Config.ServeAgentsHttp,
		"title":               "long queries",
		"activePage":          "queries",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserId(req, user),
		"autoshow_problems":   false,
		"filter":              filter,
	})
}

func (this *HttpWeb) Audit(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	page, err := strconv.Atoi(params["page"])
	if err != nil {
		page = 0
	}

	r.HTML(200, "templates/audit", map[string]interface{}{
		"agentsHttpActive":    config.Config.ServeAgentsHttp,
		"title":               "audit",
		"activePage":          "audit",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserId(req, user),
		"autoshow_problems":   false,
		"page":                page,
		"auditHostname":       params["host"],
		"auditPort":           params["port"],
	})
}

func (this *HttpWeb) AuditRecovery(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	page, err := strconv.Atoi(params["page"])
	if err != nil {
		page = 0
	}
	recoveryId, err := strconv.ParseInt(params["id"], 10, 0)
	if err != nil {
		recoveryId = 0
	}

	r.HTML(200, "templates/audit_recovery", map[string]interface{}{
		"agentsHttpActive":    config.Config.ServeAgentsHttp,
		"title":               "audit-recovery",
		"activePage":          "audit",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserId(req, user),
		"autoshow_problems":   false,
		"page":                page,
		"clusterName":         params["clusterName"],
		"recoveryId":          recoveryId,
	})
}

func (this *HttpWeb) AuditFailureDetection(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	page, err := strconv.Atoi(params["page"])
	if err != nil {
		page = 0
	}
	detectionId, err := strconv.ParseInt(params["id"], 10, 0)
	if err != nil {
		detectionId = 0
	}

	r.HTML(200, "templates/audit_failure_detection", map[string]interface{}{
		"agentsHttpActive":    config.Config.ServeAgentsHttp,
		"title":               "audit-failure-detection",
		"activePage":          "audit",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserId(req, user),
		"autoshow_problems":   false,
		"page":                page,
		"detectionId":         detectionId,
	})
}

func (this *HttpWeb) Agents(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	r.HTML(200, "templates/agents", map[string]interface{}{
		"agentsHttpActive":    config.Config.ServeAgentsHttp,
		"title":               "agents",
		"activePage":          "agents",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserId(req, user),
		"autoshow_problems":   false,
	})
}

func (this *HttpWeb) Agent(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	r.HTML(200, "templates/agent", map[string]interface{}{
		"agentsHttpActive":    config.Config.ServeAgentsHttp,
		"title":               "agent",
		"activePage":          "agents",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserId(req, user),
		"autoshow_problems":   false,
		"agentHost":           params["host"],
	})
}

func (this *HttpWeb) AgentSeedDetails(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	r.HTML(200, "templates/agent_seed_details", map[string]interface{}{
		"agentsHttpActive":    config.Config.ServeAgentsHttp,
		"title":               "agent seed details",
		"activePage":          "agents",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserId(req, user),
		"autoshow_problems":   false,
		"seedId":              params["seedId"],
	})
}

func (this *HttpWeb) Seeds(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	r.HTML(200, "templates/seeds", map[string]interface{}{
		"agentsHttpActive":    config.Config.ServeAgentsHttp,
		"title":               "seeds",
		"activePage":          "agents",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserId(req, user),
		"autoshow_problems":   false,
	})
}

func (this *HttpWeb) Home(params martini.Params, r render.Render, req *http.Request, user auth.User) {

	r.HTML(200, "templates/home", map[string]interface{}{
		"agentsHttpActive":    config.Config.ServeAgentsHttp,
		"title":               "home",
		"activePage":          "home",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserId(req, user),
		"autoshow_problems":   false,
	})
}

func (this *HttpWeb) About(params martini.Params, r render.Render, req *http.Request, user auth.User) {

	r.HTML(200, "templates/about", map[string]interface{}{
		"agentsHttpActive":    config.Config.ServeAgentsHttp,
		"title":               "about",
		"activePage":          "home",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserId(req, user),
		"autoshow_problems":   false,
	})
}

func (this *HttpWeb) KeepCalm(params martini.Params, r render.Render, req *http.Request, user auth.User) {

	r.HTML(200, "templates/keep-calm", map[string]interface{}{
		"agentsHttpActive":    config.Config.ServeAgentsHttp,
		"title":               "Keep Calm",
		"activePage":          "home",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserId(req, user),
		"autoshow_problems":   false,
	})
}

func (this *HttpWeb) FAQ(params martini.Params, r render.Render, req *http.Request, user auth.User) {

	r.HTML(200, "templates/faq", map[string]interface{}{
		"agentsHttpActive":    config.Config.ServeAgentsHttp,
		"title":               "FAQ",
		"activePage":          "home",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserId(req, user),
		"autoshow_problems":   false,
	})
}

func (this *HttpWeb) Status(params martini.Params, r render.Render, req *http.Request, user auth.User) {

	r.HTML(200, "templates/status", map[string]interface{}{
		"agentsHttpActive":    config.Config.ServeAgentsHttp,
		"title":               "status",
		"activePage":          "home",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserId(req, user),
		"autoshow_problems":   false,
	})
}

func (this *HttpWeb) RegisterRequests(m *martini.ClassicMartini) {
	m.Get("/web/access-token", this.AccessToken)
	m.Get("/", this.Clusters)
	m.Get("/web", this.Clusters)
	m.Get("/web/home", this.About)
	m.Get("/web/about", this.About)
	m.Get("/web/keep-calm", this.KeepCalm)
	m.Get("/web/faq", this.FAQ)
	m.Get("/web/status", this.Status)
	m.Get("/web/clusters", this.Clusters)
	m.Get("/web/clusters-analysis", this.ClustersAnalysis)
	m.Get("/web/cluster/:clusterName", this.Cluster)
	m.Get("/web/cluster/alias/:clusterAlias", this.ClusterByAlias)
	m.Get("/web/cluster/instance/:host/:port", this.ClusterByInstance)
	m.Get("/web/cluster-pools/:clusterName", this.ClusterPools)
	m.Get("/web/search/:searchString", this.Search)
	m.Get("/web/search", this.Search)
	m.Get("/web/discover", this.Discover)
	m.Get("/web/long-queries", this.LongQueries)
	m.Get("/web/audit", this.Audit)
	m.Get("/web/audit/:page", this.Audit)
	m.Get("/web/audit/instance/:host/:port", this.Audit)
	m.Get("/web/audit/instance/:host/:port/:page", this.Audit)
	m.Get("/web/audit-recovery", this.AuditRecovery)
	m.Get("/web/audit-recovery/:page", this.AuditRecovery)
	m.Get("/web/audit-recovery/id/:id", this.AuditRecovery)
	m.Get("/web/audit-recovery/cluster/:clusterName", this.AuditRecovery)
	m.Get("/web/audit-recovery/cluster/:clusterName/:page", this.AuditRecovery)
	m.Get("/web/audit-failure-detection", this.AuditFailureDetection)
	m.Get("/web/audit-failure-detection/:page", this.AuditFailureDetection)
	m.Get("/web/audit-failure-detection/id/:id", this.AuditFailureDetection)
	m.Get("/web/agents", this.Agents)
	m.Get("/web/agent/:host", this.Agent)
	m.Get("/web/seed-details/:seedId", this.AgentSeedDetails)
	m.Get("/web/seeds", this.Seeds)

	this.RegisterDebug(m)
}

func (this *HttpWeb) RegisterDebug(m *martini.ClassicMartini) {

	m.Get("/debug/vars", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		fmt.Fprintf(w, "{\n")
		first := true
		expvar.Do(func(kv expvar.KeyValue) {
			if !first {
				fmt.Fprintf(w, ",\n")
			}
			first = false
			fmt.Fprintf(w, "%q: %s", kv.Key, kv.Value)
		})
		fmt.Fprintf(w, "\n}\n")
	})

	m.Get("/debug/pprof", pprof.Index)
	m.Get("/debug/pprof/cmdline", pprof.Cmdline)
	m.Get("/debug/pprof/profile", pprof.Profile)
	m.Get("/debug/pprof/symbol", pprof.Symbol)
	m.Post("/debug/pprof/symbol", pprof.Symbol)
	m.Get("/debug/pprof/block", pprof.Handler("block").ServeHTTP)
	m.Get("/debug/pprof/heap", pprof.Handler("heap").ServeHTTP)
	m.Get("/debug/pprof/goroutine", pprof.Handler("goroutine").ServeHTTP)
	m.Get("/debug/pprof/threadcreate", pprof.Handler("threadcreate").ServeHTTP)
}
