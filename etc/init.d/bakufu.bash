#!/bin/bash
# bakufu daemon
# chkconfig: 345 20 80
# description: bakufu daemon
# processname: bakufu
#
#
#

DAEMON_PATH="/usr/local/bakufu"

DAEMON=bakufu
DAEMONOPTS="--verbose http"

NAME=bakufu
DESC="bakufu: MySQL replication management and visualization"
PIDFILE=/var/run/$NAME.pid
SCRIPTNAME=/etc/init.d/$NAME

ulimit -n 16384

[ -f /etc/bakufu_profile ] && . /etc/bakufu_profile

case "$1" in
	start)
	printf "%-50s" "Starting $NAME..."
	cd $DAEMON_PATH
	PID=$(./$DAEMON $DAEMONOPTS >> /var/log/${NAME}.log 2>&1 & echo $!)
	#echo "Saving PID" $PID " to " $PIDFILE
	if [ -z $PID ]; then
		printf "%s\n" "Fail"
		exit 1
	elif [ -z "$(ps axf | awk '{print $1}' | grep ${PID})" ]; then
		printf "%s\n" "Fail"
		exit 1
	else
		echo $PID > $PIDFILE
		printf "%s\n" "Ok"
	fi
	;;
	status)
	printf "%-50s" "Checking $NAME..."
	if [ -f $PIDFILE ]; then
		PID=$(cat $PIDFILE)
		if [ -z "$(ps axf | awk '{print $1}' | grep ${PID})" ]; then
		printf "%s\n" "Process dead but pidfile exists"
		exit 1
		else
		echo "Running"
		fi
	else
		printf "%s\n" "Service not running"
		exit 1
	fi
	;;
	stop)
	printf "%-50s" "Stopping $NAME"
	PID=$(cat $PIDFILE)
	cd $DAEMON_PATH
	if [ -f $PIDFILE ]; then
		kill -TERM $PID
		printf "%s\n" "Ok"
		rm -f $PIDFILE
	else
		printf "%s\n" "pidfile not found"
		exit 1
	fi
	;;
	restart)
	$0 stop
	$0 start
	;;
	reload)
	PID=$(cat $PIDFILE)
	cd $DAEMON_PATH
	if [ -f $PIDFILE ]; then
		kill -HUP $PID
		printf "%s\n" "Ok"
	else
		printf "%s\n" "pidfile not found"
		exit 1
	fi
	;;
	*)
	echo "Usage: $0 {status|start|stop|restart|reload}"
	exit 1
esac
