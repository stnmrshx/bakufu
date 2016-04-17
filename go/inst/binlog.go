/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package inst

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var detachPattern *regexp.Regexp

func init() {
	detachPattern, _ = regexp.Compile(`//([^/:]+):([\d]+)`) // e.g. `//binlog.01234:567890`
}

type BinlogType int

const (
	BinaryLog BinlogType = iota
	RelayLog
)

type BinlogCoordinates struct {
	LogFile string
	LogPos  int64
	Type    BinlogType
}

func ParseBinlogCoordinates(logFileLogPos string) (*BinlogCoordinates, error) {
	tokens := strings.SplitN(logFileLogPos, ":", 2)
	if len(tokens) != 2 {
		return nil, fmt.Errorf("ParseBinlogCoordinates: Cannot parse BinlogCoordinates from %s. Expected format is file:pos", logFileLogPos)
	}

	if logPos, err := strconv.ParseInt(tokens[1], 10, 0); err != nil {
		return nil, fmt.Errorf("ParseBinlogCoordinates: invalid pos: %s", tokens[1])
	} else {
		return &BinlogCoordinates{LogFile: tokens[0], LogPos: logPos}, nil
	}
}

func (this *BinlogCoordinates) DisplayString() string {
	return fmt.Sprintf("%s:%d", this.LogFile, this.LogPos)
}

func (this BinlogCoordinates) String() string {
	return this.DisplayString()
}

func (this *BinlogCoordinates) Equals(other *BinlogCoordinates) bool {
	if other == nil {
		return false
	}
	return this.LogFile == other.LogFile && this.LogPos == other.LogPos && this.Type == other.Type
}

func (this *BinlogCoordinates) IsEmpty() bool {
	return this.LogFile == ""
}

func (this *BinlogCoordinates) SmallerThan(other *BinlogCoordinates) bool {
	if this.LogFile < other.LogFile {
		return true
	}
	if this.LogFile == other.LogFile && this.LogPos < other.LogPos {
		return true
	}
	return false
}

func (this *BinlogCoordinates) SmallerThanOrEquals(other *BinlogCoordinates) bool {
	if this.SmallerThan(other) {
		return true
	}
	return this.LogFile == other.LogFile && this.LogPos == other.LogPos // No Type comparison
}

func (this *BinlogCoordinates) FileSmallerThan(other *BinlogCoordinates) bool {
	return this.LogFile < other.LogFile
}

func (this *BinlogCoordinates) FileNumberDistance(other *BinlogCoordinates) int {
	thisNumber, _ := this.FileNumber()
	otherNumber, _ := other.FileNumber()
	return otherNumber - thisNumber
}

func (this *BinlogCoordinates) FileNumber() (int, int) {
	tokens := strings.Split(this.LogFile, ".")
	numPart := tokens[len(tokens)-1]
	numLen := len(numPart)
	fileNum, err := strconv.Atoi(numPart)
	if err != nil {
		return 0, 0
	}
	return fileNum, numLen
}

func (this *BinlogCoordinates) PreviousFileCoordinatesBy(offset int) (BinlogCoordinates, error) {
	result := BinlogCoordinates{LogPos: 0, Type: this.Type}

	fileNum, numLen := this.FileNumber()
	if fileNum == 0 {
		return result, errors.New("Log file number is zero, cannot detect previous file")
	}
	newNumStr := fmt.Sprintf("%d", (fileNum - offset))
	newNumStr = strings.Repeat("0", numLen-len(newNumStr)) + newNumStr

	tokens := strings.Split(this.LogFile, ".")
	tokens[len(tokens)-1] = newNumStr
	result.LogFile = strings.Join(tokens, ".")
	return result, nil
}

func (this *BinlogCoordinates) PreviousFileCoordinates() (BinlogCoordinates, error) {
	return this.PreviousFileCoordinatesBy(1)
}

func (this *BinlogCoordinates) NextFileCoordinates() (BinlogCoordinates, error) {
	result := BinlogCoordinates{LogPos: 0, Type: this.Type}

	fileNum, numLen := this.FileNumber()
	newNumStr := fmt.Sprintf("%d", (fileNum + 1))
	newNumStr = strings.Repeat("0", numLen-len(newNumStr)) + newNumStr

	tokens := strings.Split(this.LogFile, ".")
	tokens[len(tokens)-1] = newNumStr
	result.LogFile = strings.Join(tokens, ".")
	return result, nil
}

func (this *BinlogCoordinates) DetachedCoordinates() (isDetached bool, detachedLogFile string, detachedLogPos string) {
	detachedCoordinatesSubmatch := detachPattern.FindStringSubmatch(this.LogFile)
	if len(detachedCoordinatesSubmatch) == 0 {
		return false, "", ""
	}
	return true, detachedCoordinatesSubmatch[1], detachedCoordinatesSubmatch[2]
}
