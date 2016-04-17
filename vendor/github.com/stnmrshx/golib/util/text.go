/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package util

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
)

func SimpleTimeToSeconds(simpleTime string) (int, error) {
	if matched, _ := regexp.MatchString("^[0-9]+s$", simpleTime); matched {
		i, _ := strconv.Atoi(simpleTime[0 : len(simpleTime)-1])
		return i, nil
	}
	if matched, _ := regexp.MatchString("^[0-9]+m$", simpleTime); matched {
		i, _ := strconv.Atoi(simpleTime[0 : len(simpleTime)-1])
		return i * 60, nil
	}
	if matched, _ := regexp.MatchString("^[0-9]+h$", simpleTime); matched {
		i, _ := strconv.Atoi(simpleTime[0 : len(simpleTime)-1])
		return i * 60 * 60, nil
	}
	if matched, _ := regexp.MatchString("^[0-9]+d$", simpleTime); matched {
		i, _ := strconv.Atoi(simpleTime[0 : len(simpleTime)-1])
		return i * 60 * 60 * 24, nil
	}
	if matched, _ := regexp.MatchString("^[0-9]+w$", simpleTime); matched {
		i, _ := strconv.Atoi(simpleTime[0 : len(simpleTime)-1])
		return i * 60 * 60 * 24 * 7, nil
	}
	return 0, errors.New(fmt.Sprintf("Cannot parse simple time: %s", simpleTime))
}
