/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package inst

type Audit struct {
	AuditId          int64
	AuditTimestamp   string
	AuditType        string
	AuditInstanceKey InstanceKey
	Message          string
}
