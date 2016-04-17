/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

//
package app

import (
	nethttp "net/http"
	"strings"

	"github.com/go-martini/martini"
	"github.com/martini-contrib/auth"
	"github.com/martini-contrib/gzip"
	"github.com/martini-contrib/render"
	"github.com/stnmrshx/golib/log"
	"github.com/stnmrshx/bakufu/go/config"
	"github.com/stnmrshx/bakufu/go/http"
	"github.com/stnmrshx/bakufu/go/inst"
	"github.com/stnmrshx/bakufu/go/logic"
	"github.com/stnmrshx/bakufu/go/process"
	"github.com/stnmrshx/bakufu/go/ssl"
)

func Http(discovery bool) {
	process.ContinuousRegistration(process.BakufuExecutionHttpMode, "")

	martini.Env = martini.Prod
	if config.Config.ServeAgentsHttp {
		go agentsHttp()
	}
	standardHttp(discovery)
}

func standardHttp(discovery bool) {
	m := martini.Classic()

	switch strings.ToLower(config.Config.AuthenticationMethod) {
	case "basic":
		{
			if config.Config.HTTPAuthUser == "" {
				log.Warning("AuthenticationMethod is configured as 'basic' but HTTPAuthUser undefined. Running without authentication.")
			}
			m.Use(auth.Basic(config.Config.HTTPAuthUser, config.Config.HTTPAuthPassword))
		}
	case "multi":
		{
			if config.Config.HTTPAuthUser == "" {
				log.Fatal("AuthenticationMethod is configured as 'multi' but HTTPAuthUser undefined")
			}

			m.Use(auth.BasicFunc(func(username, password string) bool {
				if username == "readonly" {
					return true
				}
				return auth.SecureCompare(username, config.Config.HTTPAuthUser) && auth.SecureCompare(password, config.Config.HTTPAuthPassword)
			}))
		}
	default:
		{
			m.Map(auth.User(""))
		}
	}

	m.Use(gzip.All())
	m.Use(render.Renderer(render.Options{
		Directory:       "resources",
		Layout:          "templates/layout",
		HTMLContentType: "text/html",
	}))
	m.Use(martini.Static("resources/public"))
	if config.Config.UseMutualTLS {
		m.Use(ssl.VerifyOUs(config.Config.SSLValidOUs))
	}

	inst.SetMaintenanceOwner(process.ThisHostname)

	if discovery {
		log.Info("Starting Discovery")
		go logic.ContinuousDiscovery()
	}
	inst.ReadClusterAliases()

	log.Info("Registering endpoints")
	http.API.RegisterRequests(m)
	http.Web.RegisterRequests(m)

	if config.Config.UseSSL {
		log.Info("Starting HTTPS listener")
		tlsConfig, err := ssl.NewTLSConfig(config.Config.SSLCAFile, config.Config.UseMutualTLS)
		if err != nil {
			log.Fatale(err)
		}
		tlsConfig.InsecureSkipVerify = config.Config.SSLSkipVerify
		if err = ssl.AppendKeyPair(tlsConfig, config.Config.SSLCertFile, config.Config.SSLPrivateKeyFile); err != nil {
			log.Fatale(err)
		}
		if err = ssl.ListenAndServeTLS(config.Config.ListenAddress, m, tlsConfig); err != nil {
			log.Fatale(err)
		}
	} else {
		log.Info("Starting HTTP listener")
		if err := nethttp.ListenAndServe(config.Config.ListenAddress, m); err != nil {
			log.Fatale(err)
		}
	}
	log.Info("Web server started")
}

func agentsHttp() {
	m := martini.Classic()
	m.Use(gzip.All())
	m.Use(render.Renderer())
	if config.Config.AgentsUseMutualTLS {
		m.Use(ssl.VerifyOUs(config.Config.AgentSSLValidOUs))
	}

	log.Info("Starting agents listener")

	go logic.ContinuousAgentsPoll()

	http.AgentsAPI.RegisterRequests(m)

	if config.Config.AgentsUseSSL {
		log.Info("Starting agent HTTPS listener")
		tlsConfig, err := ssl.NewTLSConfig(config.Config.AgentSSLCAFile, config.Config.AgentsUseMutualTLS)
		if err != nil {
			log.Fatale(err)
		}
		tlsConfig.InsecureSkipVerify = config.Config.AgentSSLSkipVerify
		if err = ssl.AppendKeyPair(tlsConfig, config.Config.AgentSSLCertFile, config.Config.AgentSSLPrivateKeyFile); err != nil {
			log.Fatale(err)
		}
		if err = ssl.ListenAndServeTLS(config.Config.AgentsServerPort, m, tlsConfig); err != nil {
			log.Fatale(err)
		}
	} else {
		log.Info("Starting agent HTTP listener")
		if err := nethttp.ListenAndServe(config.Config.AgentsServerPort, m); err != nil {
			log.Fatale(err)
		}
	}
	log.Info("Agent server started")
}
