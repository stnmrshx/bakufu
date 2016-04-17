/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package inst

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/stnmrshx/golib/log"
	"github.com/stnmrshx/golib/math"
	"github.com/stnmrshx/golib/sqlutils"
	"github.com/stnmrshx/bakufu/go/config"
	"github.com/stnmrshx/bakufu/go/db"
	"github.com/pmylund/go-cache"
)

const maxEmptyBinlogFiles int = 10
const maxEventInfoDisplayLength int = 200

var instanceBinlogEntryCache = cache.New(time.Duration(10)*time.Minute, time.Minute)

func compilePseudoGTIDPattern() (pseudoGTIDRegexp *regexp.Regexp, err error) {
	log.Debugf("PseudoGTIDPatternIsFixedSubstring: %+v", config.Config.PseudoGTIDPatternIsFixedSubstring)
	if config.Config.PseudoGTIDPatternIsFixedSubstring {
		return nil, nil
	}
	log.Debugf("Compiling PseudoGTIDPattern")
	return regexp.Compile(config.Config.PseudoGTIDPattern)
}

func pseudoGTIDMatches(pseudoGTIDRegexp *regexp.Regexp, binlogEntryInfo string) (found bool) {
	if config.Config.PseudoGTIDPatternIsFixedSubstring {
		return strings.Contains(binlogEntryInfo, config.Config.PseudoGTIDPattern)
	}
	return pseudoGTIDRegexp.MatchString(binlogEntryInfo)
}

func getInstanceBinlogEntryKey(instance *Instance, entry string) string {
	return fmt.Sprintf("%s;%s", instance.Key.DisplayString(), entry)
}

func getLastPseudoGTIDEntryInBinlog(pseudoGTIDRegexp *regexp.Regexp, instanceKey *InstanceKey, binlog string, binlogType BinlogType, minCoordinates *BinlogCoordinates, maxCoordinates *BinlogCoordinates) (*BinlogCoordinates, string, error) {
	if binlog == "" {
		return nil, "", log.Errorf("getLastPseudoGTIDEntryInBinlog: empty binlog file name for %+v. maxCoordinates = %+v", *instanceKey, maxCoordinates)
	}
	binlogCoordinates := BinlogCoordinates{LogFile: binlog, LogPos: 0, Type: binlogType}
	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return nil, "", err
	}

	moreRowsExpected := true
	var nextPos int64 = 0
	var relyLogMinPos int64 = 0
	if minCoordinates != nil && minCoordinates.LogFile == binlog {
		log.Debugf("getLastPseudoGTIDEntryInBinlog: starting with %+v", *minCoordinates)
		nextPos = minCoordinates.LogPos
		relyLogMinPos = minCoordinates.LogPos
	}
	step := 0

	entryText := ""
	for moreRowsExpected {
		query := ""
		if binlogCoordinates.Type == BinaryLog {
			query = fmt.Sprintf("show binlog events in '%s' FROM %d LIMIT %d", binlog, nextPos, config.Config.BinlogEventsChunkSize)
		} else {
			query = fmt.Sprintf("show relaylog events in '%s' FROM %d LIMIT %d,%d", binlog, relyLogMinPos, (step * config.Config.BinlogEventsChunkSize), config.Config.BinlogEventsChunkSize)
		}

		moreRowsExpected = false
		queryRowsFunc := sqlutils.QueryRowsMap
		if config.Config.BufferBinlogEvents {
			queryRowsFunc = sqlutils.QueryRowsMapBuffered
		}
		err = queryRowsFunc(db, query, func(m sqlutils.RowMap) error {
			moreRowsExpected = true
			nextPos = m.GetInt64("End_log_pos")
			binlogEntryInfo := m.GetString("Info")
			if pseudoGTIDMatches(pseudoGTIDRegexp, binlogEntryInfo) {
				if maxCoordinates != nil && maxCoordinates.SmallerThan(&BinlogCoordinates{LogFile: binlog, LogPos: m.GetInt64("Pos")}) {
					moreRowsExpected = false
					return nil
				}
				binlogCoordinates.LogPos = m.GetInt64("Pos")
				entryText = binlogEntryInfo
			}
			return nil
		})
		if err != nil {
			return nil, "", err
		}
		step++
	}

	if binlogCoordinates.LogPos == 0 {
		return nil, "", nil
	}
	return &binlogCoordinates, entryText, err
}

func getLastPseudoGTIDEntryInInstance(instance *Instance, minBinlogCoordinates *BinlogCoordinates, maxBinlogCoordinates *BinlogCoordinates, exhaustiveSearch bool) (*BinlogCoordinates, string, error) {
	pseudoGTIDRegexp, err := compilePseudoGTIDPattern()
	if err != nil {
		return nil, "", err
	}
	currentBinlog := instance.SelfBinlogCoordinates

	err = nil
	for err == nil {
		log.Debugf("Searching for latest pseudo gtid entry in binlog %+v of %+v", currentBinlog.LogFile, instance.Key)
		resultCoordinates, entryInfo, err := getLastPseudoGTIDEntryInBinlog(pseudoGTIDRegexp, &instance.Key, currentBinlog.LogFile, BinaryLog, minBinlogCoordinates, maxBinlogCoordinates)
		if err != nil {
			return nil, "", err
		}
		if resultCoordinates != nil {
			log.Debugf("Found pseudo gtid entry in %+v, %+v", instance.Key, resultCoordinates)
			return resultCoordinates, entryInfo, err
		}
		if !exhaustiveSearch {
			log.Debugf("Not an exhaustive search. Bailing out")
			break
		}
		if minBinlogCoordinates != nil && minBinlogCoordinates.LogFile == currentBinlog.LogFile {
			minBinlogCoordinates = nil
			log.Debugf("Heuristic binlog search failed; continuing exhaustive search")
		} else {
			currentBinlog, err = currentBinlog.PreviousFileCoordinates()
			if err != nil {
				return nil, "", err
			}
		}
	}
	return nil, "", log.Errorf("Cannot find pseudo GTID entry in binlogs of %+v", instance.Key)
}

func getLastPseudoGTIDEntryInRelayLogs(instance *Instance, minBinlogCoordinates *BinlogCoordinates, recordedInstanceRelayLogCoordinates BinlogCoordinates, exhaustiveSearch bool) (*BinlogCoordinates, string, error) {
	pseudoGTIDRegexp, err := compilePseudoGTIDPattern()
	if err != nil {
		return nil, "", err
	}

	currentRelayLog := recordedInstanceRelayLogCoordinates
	err = nil
	for err == nil {
		log.Debugf("Searching for latest pseudo gtid entry in relaylog %+v of %+v, up to pos %+v", currentRelayLog.LogFile, instance.Key, recordedInstanceRelayLogCoordinates)
		if resultCoordinates, entryInfo, err := getLastPseudoGTIDEntryInBinlog(pseudoGTIDRegexp, &instance.Key, currentRelayLog.LogFile, RelayLog, minBinlogCoordinates, &recordedInstanceRelayLogCoordinates); err != nil {
			return nil, "", err
		} else if resultCoordinates != nil {
			log.Debugf("Found pseudo gtid entry in %+v, %+v", instance.Key, resultCoordinates)
			return resultCoordinates, entryInfo, err
		}
		if !exhaustiveSearch {
			break
		}
		if minBinlogCoordinates != nil && minBinlogCoordinates.LogFile == currentRelayLog.LogFile {
			minBinlogCoordinates = nil
			log.Debugf("Heuristic relaylog search failed; continuing exhaustive search")
		} else {
			currentRelayLog, err = currentRelayLog.PreviousFileCoordinates()
		}
	}
	return nil, "", log.Errorf("Cannot find pseudo GTID entry in relay logs of %+v", instance.Key)
}

func SearchEntryInBinlog(pseudoGTIDRegexp *regexp.Regexp, instanceKey *InstanceKey, binlog string, entryText string, monotonicPseudoGTIDEntries bool, minBinlogCoordinates *BinlogCoordinates) (BinlogCoordinates, bool, error) {
	binlogCoordinates := BinlogCoordinates{LogFile: binlog, LogPos: 0, Type: BinaryLog}
	if binlog == "" {
		return binlogCoordinates, false, log.Errorf("SearchEntryInBinlog: empty binlog file name for %+v", *instanceKey)
	}

	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return binlogCoordinates, false, err
	}

	moreRowsExpected := true
	skipRestOfBinlog := false
	alreadyMatchedAscendingPseudoGTID := false
	var nextPos int64 = 0
	if minBinlogCoordinates != nil && minBinlogCoordinates.LogFile == binlog {
		log.Debugf("SearchEntryInBinlog: starting with %+v", *minBinlogCoordinates)
		nextPos = minBinlogCoordinates.LogPos
	}

	for moreRowsExpected {
		query := fmt.Sprintf("show binlog events in '%s' FROM %d LIMIT %d", binlog, nextPos, config.Config.BinlogEventsChunkSize)
		moreRowsExpected = false
		queryRowsFunc := sqlutils.QueryRowsMap
		if config.Config.BufferBinlogEvents {
			queryRowsFunc = sqlutils.QueryRowsMapBuffered
		}
		err = queryRowsFunc(db, query, func(m sqlutils.RowMap) error {
			if binlogCoordinates.LogPos != 0 {
				skipRestOfBinlog = true
				return nil
			}
			if skipRestOfBinlog {
				return nil
			}
			moreRowsExpected = true
			nextPos = m.GetInt64("End_log_pos")
			binlogEntryInfo := m.GetString("Info")
			if binlogEntryInfo == entryText {
				binlogCoordinates.LogPos = m.GetInt64("Pos")
			} else if monotonicPseudoGTIDEntries && !alreadyMatchedAscendingPseudoGTID {
				if pseudoGTIDMatches(pseudoGTIDRegexp, binlogEntryInfo) {
					alreadyMatchedAscendingPseudoGTID = true
					log.Debugf("Matched ascending Pseudo-GTID entry in %+v", binlog)
					if binlogEntryInfo > entryText {
						log.Debugf(`Pseudo GTID entries are monotonic and we hit "%+v" > "%+v"; skipping binlog %+v`, m.GetString("Info"), entryText, binlogCoordinates.LogFile)
						skipRestOfBinlog = true
						return nil
					}
				}
			}
			return nil
		})
		if err != nil {
			return binlogCoordinates, (binlogCoordinates.LogPos != 0), err
		}
		if skipRestOfBinlog {
			return binlogCoordinates, (binlogCoordinates.LogPos != 0), err
		}
	}

	return binlogCoordinates, (binlogCoordinates.LogPos != 0), err
}

func SearchEntryInInstanceBinlogs(instance *Instance, entryText string, monotonicPseudoGTIDEntries bool, minBinlogCoordinates *BinlogCoordinates) (*BinlogCoordinates, error) {
	pseudoGTIDRegexp, err := compilePseudoGTIDPattern()
	if err != nil {
		return nil, err
	}
	cacheKey := getInstanceBinlogEntryKey(instance, entryText)
	coords, found := instanceBinlogEntryCache.Get(cacheKey)
	if found {
		log.Debugf("Found instance Pseudo GTID entry coordinates in cache: %+v, %+v, %+v", instance.Key, entryText, coords)
		return coords.(*BinlogCoordinates), nil
	}

	log.Debugf("Searching for given pseudo gtid entry in %+v. monotonicPseudoGTIDEntries=%+v", instance.Key, monotonicPseudoGTIDEntries)
	currentBinlog := instance.SelfBinlogCoordinates
	err = nil
	for {
		log.Debugf("Searching for given pseudo gtid entry in binlog %+v of %+v", currentBinlog.LogFile, instance.Key)
		if instance.SlaveRunning() {
			for {
				log.Debugf("%+v is a replicating slave. Verifying lag", instance.Key)
				instance, err = ReadTopologyInstance(&instance.Key)
				if err != nil {
					break
				}
				if instance.HasReasonableMaintenanceReplicationLag() {
					break
				}
				log.Debugf("lag is too high on %+v. Throttling the search for pseudo gtid entry", instance.Key)
				time.Sleep(time.Duration(config.Config.ReasonableMaintenanceReplicationLagSeconds) * time.Second)
			}
		}
		var resultCoordinates BinlogCoordinates
		var found bool = false
		resultCoordinates, found, err = SearchEntryInBinlog(pseudoGTIDRegexp, &instance.Key, currentBinlog.LogFile, entryText, monotonicPseudoGTIDEntries, minBinlogCoordinates)
		if err != nil {
			break
		}
		if found {
			log.Debugf("Matched entry in %+v: %+v", instance.Key, resultCoordinates)
			instanceBinlogEntryCache.Set(cacheKey, &resultCoordinates, 0)
			return &resultCoordinates, nil
		}
		if minBinlogCoordinates != nil && minBinlogCoordinates.LogFile == currentBinlog.LogFile {
			log.Debugf("Heuristic master binary logs search failed; continuing exhaustive search")
			minBinlogCoordinates = nil
		} else {
			currentBinlog, err = currentBinlog.PreviousFileCoordinates()
			if err != nil {
				break
			}
			log.Debugf("- Will move next to binlog %+v", currentBinlog.LogFile)
		}
	}

	return nil, log.Errorf("Cannot match pseudo GTID entry in binlogs of %+v; err: %+v", instance.Key, err)
}

func readBinlogEventsChunk(instanceKey *InstanceKey, startingCoordinates BinlogCoordinates) ([]BinlogEvent, error) {
	events := []BinlogEvent{}
	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return events, err
	}
	commandToken := math.TernaryString(startingCoordinates.Type == BinaryLog, "binlog", "relaylog")
	if startingCoordinates.LogFile == "" {
		return events, log.Errorf("readBinlogEventsChunk: empty binlog file name for %+v.", *instanceKey)
	}
	query := fmt.Sprintf("show %s events in '%s' FROM %d LIMIT %d", commandToken, startingCoordinates.LogFile, startingCoordinates.LogPos, config.Config.BinlogEventsChunkSize)
	err = sqlutils.QueryRowsMap(db, query, func(m sqlutils.RowMap) error {
		binlogEvent := BinlogEvent{}
		binlogEvent.Coordinates.LogFile = m.GetString("Log_name")
		binlogEvent.Coordinates.LogPos = m.GetInt64("Pos")
		binlogEvent.Coordinates.Type = startingCoordinates.Type
		binlogEvent.NextEventPos = m.GetInt64("End_log_pos")
		binlogEvent.EventType = m.GetString("Event_type")
		binlogEvent.Info = m.GetString("Info")

		events = append(events, binlogEvent)
		return nil
	})
	return events, err
}

func getNextBinlogEventsChunk(instance *Instance, startingCoordinates BinlogCoordinates, numEmptyBinlogs int) ([]BinlogEvent, error) {
	if numEmptyBinlogs > maxEmptyBinlogFiles {
		log.Debugf("Reached maxEmptyBinlogFiles (%d) at %+v", maxEmptyBinlogFiles, startingCoordinates)
		return []BinlogEvent{}, nil
	}
	coordinatesExceededCurrent := false
	switch startingCoordinates.Type {
	case BinaryLog:
		coordinatesExceededCurrent = instance.SelfBinlogCoordinates.FileSmallerThan(&startingCoordinates)
	case RelayLog:
		coordinatesExceededCurrent = instance.RelaylogCoordinates.FileSmallerThan(&startingCoordinates)
	}
	if coordinatesExceededCurrent {
		log.Debugf("Coordinates overflow: %+v; terminating search", startingCoordinates)
		return []BinlogEvent{}, nil
	}
	events, err := readBinlogEventsChunk(&instance.Key, startingCoordinates)
	if err != nil {
		return events, err
	}
	if len(events) > 0 {
		log.Debugf("Returning %d events at %+v", len(events), startingCoordinates)
		return events, nil
	}

	if nextCoordinates, err := instance.GetNextBinaryLog(startingCoordinates); err == nil {
		log.Debugf("Recursing into %+v", nextCoordinates)
		return getNextBinlogEventsChunk(instance, nextCoordinates, numEmptyBinlogs+1)
	}
	return events, err
}

func GetNextBinlogCoordinatesToMatch(instance *Instance, instanceCoordinates BinlogCoordinates, recordedInstanceRelayLogCoordinates BinlogCoordinates, maxBinlogCoordinates *BinlogCoordinates,
	other *Instance, otherCoordinates BinlogCoordinates) (*BinlogCoordinates, int, error) {

	fetchNextEvents := func(binlogCoordinates BinlogCoordinates) ([]BinlogEvent, error) {
		return getNextBinlogEventsChunk(instance, binlogCoordinates, 0)
	}
	instanceCursor := NewBinlogEventCursor(instanceCoordinates, fetchNextEvents)

	fetchOtherNextEvents := func(binlogCoordinates BinlogCoordinates) ([]BinlogEvent, error) {
		return getNextBinlogEventsChunk(other, binlogCoordinates, 0)
	}
	otherCursor := NewBinlogEventCursor(otherCoordinates, fetchOtherNextEvents)

	var beautifyCoordinatesLength int = 0
	rpad := func(s string, length int) string {
		if len(s) >= length {
			return s
		}
		return fmt.Sprintf("%s%s", s, strings.Repeat(" ", length-len(s)))
	}

	var lastConsumedEventCoordinates BinlogCoordinates
	var countMatchedEvents int = 0
	for {
		var instanceEventInfo string
		var otherEventInfo string
		var eventCoordinates BinlogCoordinates
		var otherEventCoordinates BinlogCoordinates
		{
			event, err := instanceCursor.nextRealEvent(0)
			if err != nil {
				return nil, 0, log.Errore(err)
			}
			if event != nil {
				lastConsumedEventCoordinates = event.Coordinates
			}

			switch instanceCoordinates.Type {
			case BinaryLog:
				if event == nil {
					targetMatchCoordinates, err := otherCursor.getNextCoordinates()
					if err != nil {
						return nil, 0, log.Errore(err)
					}
					nextCoordinates, _ := instanceCursor.getNextCoordinates()
					if nextCoordinates.SmallerThan(&instance.SelfBinlogCoordinates) {
						return nil, 0, log.Errorf("Unexpected problem: instance binlog iteration ended before self coordinates. Ended with: %+v, self coordinates: %+v", nextCoordinates, instance.SelfBinlogCoordinates)
					}
					log.Debugf("Reached end of binary logs for instance, at %+v. Other coordinates: %+v", nextCoordinates, targetMatchCoordinates)
					return &targetMatchCoordinates, countMatchedEvents, nil
				}
			case RelayLog:
				endOfScan := false
				if event == nil {
					endOfScan = true
					log.Debugf("Reached end of relay log at %+v", recordedInstanceRelayLogCoordinates)
				} else if recordedInstanceRelayLogCoordinates.Equals(&event.Coordinates) {
					endOfScan = true
					log.Debugf("Reached slave relay log coordinates at %+v", recordedInstanceRelayLogCoordinates)
				} else if recordedInstanceRelayLogCoordinates.SmallerThan(&event.Coordinates) {
					return nil, 0, log.Errorf("Unexpected problem: relay log scan passed relay log position without hitting it. Ended with: %+v, relay log position: %+v", event.Coordinates, recordedInstanceRelayLogCoordinates)
				}
				if endOfScan {
					targetMatchCoordinates, err := otherCursor.getNextCoordinates()
					if err != nil {
						log.Debugf("Cannot otherCursor.getNextCoordinates(). otherCoordinates=%+v, cached events in cursor: %d; index=%d", otherCoordinates, len(otherCursor.cachedEvents), otherCursor.currentEventIndex)
						return nil, 0, log.Errore(err)
					}
					log.Debugf("Reached limit of relay logs for instance, just after %+v. Other coordinates: %+v", lastConsumedEventCoordinates, targetMatchCoordinates)
					return &targetMatchCoordinates, countMatchedEvents, nil
				}
			}

			instanceEventInfo = event.Info
			eventCoordinates = event.Coordinates
			coordinatesStr := fmt.Sprintf("%+v", event.Coordinates)
			if len(coordinatesStr) > beautifyCoordinatesLength {
				beautifyCoordinatesLength = len(coordinatesStr)
			}
			log.Debugf("> %+v %+v; %+v", rpad(coordinatesStr, beautifyCoordinatesLength), event.EventType, strings.Split(strings.TrimSpace(instanceEventInfo), "\n")[0])
		}
		{
			event, err := otherCursor.nextRealEvent(0)
			if err != nil {
				return nil, 0, log.Errore(err)
			}
			if event == nil {
				return nil, 0, log.Errorf("Unexpected end of binary logs for assumed master (%+v). This means the instance which attempted to be a slave (%+v) was more advanced. Try the other way round", other.Key, instance.Key)
			}
			otherEventInfo = event.Info
			otherEventCoordinates = event.Coordinates
			coordinatesStr := fmt.Sprintf("%+v", event.Coordinates)
			if len(coordinatesStr) > beautifyCoordinatesLength {
				beautifyCoordinatesLength = len(coordinatesStr)
			}
			log.Debugf("< %+v %+v; %+v", rpad(coordinatesStr, beautifyCoordinatesLength), event.EventType, strings.Split(strings.TrimSpace(otherEventInfo), "\n")[0])
		}
		if instanceEventInfo != otherEventInfo {
			return nil, 0, log.Errorf("Mismatching entries, aborting: %+v <-> %+v", instanceEventInfo, otherEventInfo)
		}
		countMatchedEvents++
		if maxBinlogCoordinates != nil {
			if eventCoordinates.Equals(maxBinlogCoordinates) {
				log.Debugf("maxBinlogCoordinates specified as %+v and reached. Stopping", *maxBinlogCoordinates)
				return &otherEventCoordinates, countMatchedEvents, nil
			} else if maxBinlogCoordinates.SmallerThan(&eventCoordinates) {
				return nil, 0, log.Errorf("maxBinlogCoordinates (%+v) exceeded but not met", *maxBinlogCoordinates)
			}
		}
	}
}
