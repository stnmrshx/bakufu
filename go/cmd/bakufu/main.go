/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/stnmrshx/golib/log"
	"github.com/stnmrshx/golib/math"
	"github.com/stnmrshx/bakufu/go/app"
	"github.com/stnmrshx/bakufu/go/config"
	"github.com/stnmrshx/bakufu/go/inst"
)

var AppVersion string

const prompt string = `
bakufu [-c command] [-i instance] [-d destination] [--verbose|--debug] [... cli ] | http

Citsit:
    Http mode bakufu :

        bakufu --debug http

    Semua command:

        bakufu -c help

    Command patternnyo:
        bakufu -c <command> [-i <instance.fqdn>] [-d <destination.fqdn>] [--verbose|--debug]

    -i (instance):
        instance ini formatnya "hostname" atawa "hostname:port", default portnya sih 
        3306, bisa diganti di DefaultInstancePort di config.
    -d (destination)
        destination instance, buat pindah2 replikasi kalo misalnya mau diset failover
    -c (command):
        Ya command nya ini

    Selebihnya adalah rahasia ilahi....***karna malas tulis dokumentasi...
    Kalo males rm -rf * aja ngahahaha :)))
    `

func main() {
	configFile := flag.String("config", "", "config file")
	command := flag.String("c", "", "command, required.")
	strict := flag.Bool("strict", false, "strict mode")
	instance := flag.String("i", "", "instance, host_fqdn[:port] (e.g. db.jembutan.io:3306, db.jembutan.io)")
	sibling := flag.String("s", "", "sibling instance, host_fqdn[:port]")
	destination := flag.String("d", "", "destination instance, host_fqdn[:port] (sama aja dengan -s)")
	owner := flag.String("owner", "", "operation owner")
	reason := flag.String("reason", "", "operation reason")
	duration := flag.String("duration", "", "durasi maintenis (format: 59s, 59m, 23h, 6d, 4w)")
	pattern := flag.String("pattern", "", "regex pattern")
	clusterAlias := flag.String("alias", "", "cluster alias")
	pool := flag.String("pool", "", "Pool logical name")
	hostnameFlag := flag.String("hostname", "", "Hostname/fqdn/CNAME/VIP")
	discovery := flag.Bool("discovery", true, "auto discovery mode")
	quiet := flag.Bool("quiet", false, "quiet")
	verbose := flag.Bool("verbose", false, "verbose")
	debug := flag.Bool("debug", false, "debug mode")
	stack := flag.Bool("stack", false, "add stack trace upon error")
	config.RuntimeCLIFlags.Databaseless = flag.Bool("databaseless", false, "EXPERIMENTAL! gapake backend database")
	config.RuntimeCLIFlags.SkipUnresolve = flag.Bool("skip-unresolve", false, "Skip resolving hostname")
	config.RuntimeCLIFlags.SkipUnresolveCheck = flag.Bool("skip-unresolve-check", false, "Skip/ignore mapping")
	config.RuntimeCLIFlags.Noop = flag.Bool("noop", false, "Dry run; mode kering kerontang ngahahaha")
	config.RuntimeCLIFlags.BinlogFile = flag.String("binlog", "", "Binary log file")
	config.RuntimeCLIFlags.Statement = flag.String("statement", "", "Statement/hint")
	config.RuntimeCLIFlags.GrabElection = flag.Bool("grab-election", false, "Grab leadership")
	config.RuntimeCLIFlags.PromotionRule = flag.String("promotion-rule", "prefer", "Promotion rule ")
	config.RuntimeCLIFlags.Version = flag.Bool("version", false, "Print version trus exit")
	flag.Parse()

	if *destination != "" && *sibling != "" {
		log.Fatalf("-s and -d itu sama")
	}
	switch *config.RuntimeCLIFlags.PromotionRule {
	case "prefer", "neutral", "must_not":
		{
			// MMMKAAAYYY
		}
	default:
		{
			log.Fatalf("-promotion-rule only supports prefer|neutral|must_not")
		}
	}
	if *destination == "" {
		*destination = *sibling
	}

	log.SetLevel(log.ERROR)
	if *verbose {
		log.SetLevel(log.INFO)
	}
	if *debug {
		log.SetLevel(log.DEBUG)
	}
	if *stack {
		log.SetPrintStackTrace(*stack)
	}
	log.Info("starting bakufu")

	if *config.RuntimeCLIFlags.Version {
		fmt.Println(AppVersion)
		return
	}

	runtime.GOMAXPROCS(math.MinInt(4, runtime.NumCPU()))

	if len(*configFile) > 0 {
		config.ForceRead(*configFile)
	} else {
		config.Read("/etc/bakufu.conf.json", "conf/bakufu.conf.json", "bakufu.conf.json")
	}
	if *config.RuntimeCLIFlags.Databaseless {
		config.Config.DatabaselessMode__experimental = true
	}
	if config.Config.Debug {
		log.SetLevel(log.DEBUG)
	}
	if *quiet {
		log.SetLevel(log.ERROR)
	}
	if config.Config.EnableSyslog {
		log.EnableSyslogWriter("bakufu")
		log.SetSyslogLevel(log.INFO)
	}
	if config.Config.AuditToSyslog {
		inst.EnableAuditSyslog()
	}
	config.RuntimeCLIFlags.ConfiguredVersion = AppVersion

	if len(flag.Args()) == 0 && *command == "" {
		fmt.Println(prompt)
		return
	}
	switch {
	case len(flag.Args()) == 0 || flag.Arg(0) == "cli":
		app.CliWrapper(*command, *strict, *instance, *destination, *owner, *reason, *duration, *pattern, *clusterAlias, *pool, *hostnameFlag)
	case flag.Arg(0) == "http":
		app.Http(*discovery)
	default:
		fmt.Fprintln(os.Stderr, `Cara Pakai:
  bakufu --options... [cli|http]
Complete commands:
  bakufu -c help
Dokumentasi:
  bakufu
`)
		os.Exit(1)
	}
}
