/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package inst

import (
	"strings"
)

type OracleGtidSet struct {
	GtidEntries [](*OracleGtidSetEntry)
}

func ParseGtidSet(gtidSet string) (res *OracleGtidSet, err error) {
	res = &OracleGtidSet{}

	gtidSet = strings.TrimSpace(gtidSet)
	if gtidSet == "" {
		return res, nil
	}
	entries := strings.Split(gtidSet, ",")
	for _, entry := range entries {
		if gtidRange, err := NewOracleGtidSetEntry(entry); err == nil {
			res.GtidEntries = append(res.GtidEntries, gtidRange)
		} else {
			return res, err
		}
	}
	return res, nil
}

func (this *OracleGtidSet) RemoveUUID(uuid string) (removed bool) {
	filteredEntries := [](*OracleGtidSetEntry){}
	for _, entry := range this.GtidEntries {
		if entry.UUID == uuid {
			removed = true
		} else {
			filteredEntries = append(filteredEntries, entry)
		}
	}
	if removed {
		this.GtidEntries = filteredEntries
	}
	return removed
}

func (this OracleGtidSet) String() string {
	tokens := []string{}
	for _, entry := range this.GtidEntries {
		tokens = append(tokens, entry.String())
	}
	return strings.Join(tokens, ",\n")
}
