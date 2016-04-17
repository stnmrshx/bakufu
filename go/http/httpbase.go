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
	"strings"

	"github.com/martini-contrib/auth"
	"github.com/stnmrshx/bakufu/go/config"
	"github.com/stnmrshx/bakufu/go/process"
)

func getProxyAuthUser(req *http.Request) string {
	for _, user := range req.Header[config.Config.AuthUserHeader] {
		return user
	}
	return ""
}

func isAuthorizedForAction(req *http.Request, user auth.User) bool {
	if config.Config.ReadOnly {
		return false
	}

	switch strings.ToLower(config.Config.AuthenticationMethod) {
	case "basic":
		{
			return true
		}
	case "multi":
		{
			if string(user) == "readonly" {
				return false
			}
			return true
		}
	case "proxy":
		{
			authUser := getProxyAuthUser(req)
			for _, configPowerAuthUser := range config.Config.PowerAuthUsers {
				if configPowerAuthUser == "*" || configPowerAuthUser == authUser {
					return true
				}
			}
			return false
		}
	case "token":
		{
			cookie, err := req.Cookie("access-token")
			if err != nil {
				return false
			}

			publicToken := strings.Split(cookie.Value, ":")[0]
			secretToken := strings.Split(cookie.Value, ":")[1]
			result, _ := process.TokenIsValid(publicToken, secretToken)
			return result
		}
	default:
		{
			return true
		}
	}
}

func authenticateToken(publicToken string, resp http.ResponseWriter) error {
	secretToken, err := process.AcquireAccessToken(publicToken)
	if err != nil {
		return err
	}
	cookieValue := fmt.Sprintf("%s:%s", publicToken, secretToken)
	cookie := &http.Cookie{Name: "access-token", Value: cookieValue, Path: "/"}
	http.SetCookie(resp, cookie)
	return nil
}

func getUserId(req *http.Request, user auth.User) string {
	if config.Config.ReadOnly {
		return ""
	}

	switch strings.ToLower(config.Config.AuthenticationMethod) {
	case "basic":
		{
			return string(user)
		}
	case "multi":
		{
			return string(user)
		}
	case "proxy":
		{
			return getProxyAuthUser(req)
		}
	case "token":
		{
			return ""
		}
	default:
		{
			return ""
		}
	}
}
