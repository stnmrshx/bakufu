/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package os

import (
	"github.com/stnmrshx/golib/log"
	"io/ioutil"
	"os"
	"os/exec"
)

func execCmd(commandText string, arguments ...string) (*exec.Cmd, string, error) {
	commandBytes := []byte(commandText)
	tmpFile, err := ioutil.TempFile("", "bakufu-process-cmd-")
	if err != nil {
		return nil, "", log.Errore(err)
	}
	ioutil.WriteFile(tmpFile.Name(), commandBytes, 0644)
	log.Debugf("execCmd: %s", commandText)
	shellArguments := append([]string{}, tmpFile.Name())
	shellArguments = append(shellArguments, arguments...)
	log.Debugf("%+v", shellArguments)
	return exec.Command("bash", shellArguments...), tmpFile.Name(), nil
}

func CommandRun(commandText string, arguments ...string) error {
	cmd, tmpFileName, err := execCmd(commandText, arguments...)
	defer os.Remove(tmpFileName)
	if err != nil {
		return log.Errore(err)
	}
	err = cmd.Run()
	if err != nil {
		return log.Errore(err)
	}
	return nil
}
