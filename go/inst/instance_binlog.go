/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package inst

import (
	"errors"
	"regexp"
	"strings"

	"github.com/stnmrshx/golib/log"
	"github.com/stnmrshx/bakufu/go/config"
)

var eventInfoTransformations map[*regexp.Regexp]string = map[*regexp.Regexp]string{
	regexp.MustCompile(`(.*) [/][*].*?[*][/](.*$)`):  "$1 $2",
	regexp.MustCompile(`(COMMIT) .*$`):               "$1",
	regexp.MustCompile(`(table_id:) [0-9]+ (.*$)`):   "$1 ### $2",
	regexp.MustCompile(`(table_id:) [0-9]+$`):        "$1 ###",
	regexp.MustCompile(` X'([0-9a-fA-F]+)' COLLATE`): " 0x$1 COLLATE",
	regexp.MustCompile(`(BEGIN GTID [^ ]+) cid=.*`):  "$1",
}

var skippedEventTypes map[string]bool = map[string]bool{
	"Format_desc": true,
	"Stop":        true,
	"Rotate":      true,
}

type BinlogEvent struct {
	Coordinates  BinlogCoordinates
	NextEventPos int64
	EventType    string
	Info         string
}

func (this *BinlogEvent) NextBinlogCoordinates() BinlogCoordinates {
	return BinlogCoordinates{LogFile: this.Coordinates.LogFile, LogPos: this.NextEventPos, Type: this.Coordinates.Type}
}

func (this *BinlogEvent) NormalizeInfo() {
	for reg, replace := range eventInfoTransformations {
		this.Info = reg.ReplaceAllString(this.Info, replace)
	}
}

const maxEmptyEventsEvents int = 10

type BinlogEventCursor struct {
	cachedEvents      []BinlogEvent
	currentEventIndex int
	fetchNextEvents   func(BinlogCoordinates) ([]BinlogEvent, error)
	nextCoordinates   BinlogCoordinates
}

func NewBinlogEventCursor(startCoordinates BinlogCoordinates, fetchNextEventsFunc func(BinlogCoordinates) ([]BinlogEvent, error)) BinlogEventCursor {
	events, _ := fetchNextEventsFunc(startCoordinates)
	var initialNextCoordinates BinlogCoordinates
	if len(events) > 0 {
		initialNextCoordinates = events[0].NextBinlogCoordinates()
	}
	return BinlogEventCursor{
		cachedEvents:      events,
		currentEventIndex: -1,
		fetchNextEvents:   fetchNextEventsFunc,
		nextCoordinates:   initialNextCoordinates,
	}
}

func (this *BinlogEventCursor) nextEvent(numEmptyEventsEvents int) (*BinlogEvent, error) {
	if numEmptyEventsEvents > maxEmptyEventsEvents {
		log.Debugf("End of logs. currentEventIndex: %d, nextCoordinates: %+v", this.currentEventIndex, this.nextCoordinates)
		return nil, nil
	}
	if len(this.cachedEvents) == 0 {
		nextFileCoordinates, err := this.nextCoordinates.NextFileCoordinates()
		if err != nil {
			return nil, err
		}
		log.Debugf("zero cached events, next file: %+v", nextFileCoordinates)
		this.cachedEvents, err = this.fetchNextEvents(nextFileCoordinates)
		if err != nil {
			return nil, err
		}
		this.currentEventIndex = -1
		return this.nextEvent(numEmptyEventsEvents + 1)
	}
	if this.currentEventIndex+1 < len(this.cachedEvents) {
		this.currentEventIndex++
		event := &this.cachedEvents[this.currentEventIndex]
		this.nextCoordinates = event.NextBinlogCoordinates()
		return event, nil
	} else {
		var err error
		this.cachedEvents, err = this.fetchNextEvents(this.cachedEvents[len(this.cachedEvents)-1].NextBinlogCoordinates())
		if err != nil {
			return nil, err
		}
		this.currentEventIndex = -1
		return this.nextEvent(numEmptyEventsEvents + 1)
	}
}

func (this *BinlogEventCursor) nextRealEvent(recursionLevel int) (*BinlogEvent, error) {
	if recursionLevel > maxEmptyEventsEvents {
		log.Debugf("End of real events")
		return nil, nil
	}
	event, err := this.nextEvent(0)
	if err != nil {
		return event, err
	}
	if event == nil {
		return event, err
	}

	if _, found := skippedEventTypes[event.EventType]; found {
		return this.nextRealEvent(recursionLevel + 1)
	}
	for _, skipSubstring := range config.Config.SkipBinlogEventsContaining {
		if strings.Index(event.Info, skipSubstring) >= 0 {
			return this.nextRealEvent(recursionLevel + 1)
		}
	}
	event.NormalizeInfo()
	return event, err
}

func (this *BinlogEventCursor) getNextCoordinates() (BinlogCoordinates, error) {
	if this.nextCoordinates.LogPos == 0 {
		return this.nextCoordinates, errors.New("Next coordinates unfound")
	}
	return this.nextCoordinates, nil
}
