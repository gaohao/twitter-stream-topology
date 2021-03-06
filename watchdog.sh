#!/bin/sh
PROCESS='twitter-stream-topology'
if [ `ps ax|grep -v grep|grep -ic $PROCESS` -lt 1 ]
then
    echo "`date`: $PROCESS is not running on `hostname`!" | mail -s "$PROCESS down!" hurui900313@gmail.com
    nohup java -jar ~/runnable/twitter-stream-topology.jar > /dev/null &
fi

exit
