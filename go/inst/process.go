/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package inst

type Process struct {
	InstanceHostname string
	InstancePort     int
	Id               int64
	User             string
	Host             string
	Db               string
	Command          string
	Time             int64
	State            string
	Info             string
	StartedAt        string
}
