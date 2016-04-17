/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package process

import (
	"github.com/stnmrshx/golib/log"
	"os"
)

var ThisHostname string

func init() {
	var err error
	ThisHostname, err = os.Hostname()
	if err != nil {
		log.Fatalf("Cannot resolve self hostname; required. Aborting. %+v", err)
	}
}
