/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package inst

import (
	"github.com/stnmrshx/golib/log"
)

type PostponedFunctionsContainer struct {
	PostponedFunctions [](func() error)
}

func NewPostponedFunctionsContainer() *PostponedFunctionsContainer {
	postponedFunctionsContainer := &PostponedFunctionsContainer{}
	postponedFunctionsContainer.PostponedFunctions = [](func() error){}
	return postponedFunctionsContainer
}

func (this *PostponedFunctionsContainer) AddPostponedFunction(f func() error) {
	this.PostponedFunctions = append(this.PostponedFunctions, f)
}

func (this *PostponedFunctionsContainer) InvokePostponed() (err error) {
	if len(this.PostponedFunctions) == 0 {
		return
	}
	log.Debugf("PostponedFunctionsContainer: invoking %+v postponed functions", len(this.PostponedFunctions))
	for _, postponedFunction := range this.PostponedFunctions {
		ferr := postponedFunction()
		if err == nil {
			err = ferr
		}
	}
	return err
}
