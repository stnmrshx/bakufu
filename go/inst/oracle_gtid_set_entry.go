/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package inst

import (
	"fmt"
	"strings"
)

type OracleGtidSetEntry struct {
	UUID   string
	Ranges string
}

func NewOracleGtidSetEntry(gtidRangeString string) (*OracleGtidSetEntry, error) {
	gtidRangeString = strings.TrimSpace(gtidRangeString)
	tokens := strings.SplitN(gtidRangeString, ":", 2)
	if len(tokens) != 2 {
		return nil, fmt.Errorf("Cannot parse OracleGtidSetEntry from %s", gtidRangeString)
	}
	if tokens[0] == "" {
		return nil, fmt.Errorf("Unexpected UUID: %s", tokens[0])
	}
	if tokens[1] == "" {
		return nil, fmt.Errorf("Unexpected GTID range: %s", tokens[1])
	}
	gtidRange := &OracleGtidSetEntry{UUID: tokens[0], Ranges: tokens[1]}
	return gtidRange, nil
}

func (this OracleGtidSetEntry) String() string {
	return fmt.Sprintf("%s:%s", this.UUID, this.Ranges)
}
