/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package config

type CLIFlags struct {
	Noop               *bool
	SkipUnresolve      *bool
	SkipUnresolveCheck *bool
	BinlogFile         *string
	Databaseless       *bool
	GrabElection       *bool
	Version            *bool
	Statement          *string
	PromotionRule      *string
	ConfiguredVersion  string
}

var RuntimeCLIFlags CLIFlags
