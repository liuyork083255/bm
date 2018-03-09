#!/bin/bash

PROCESSNUM=`ps -ef | grep com.sumscope.wf.bond.monitor.BondMonitorApplication | grep -v grep | wc -l`

if [ $PROCESSNUM -gt 0 ]
then
    echo "process is running..."
else
    start_dir=`pwd`
    cd `dirname "$0"`

    source /opt/sumscope/.ssinfo
    ENV=$env_info

    JAVA_OPT="-server -d64 -Xms2G -Xmx2G -XX:+PrintGCDateStamps -XX:+PrintGCDetails -Xloggc:./gc.log"
    SYS_OPT=" -Dlogback.configurationFile=file:../conf/logback.xml"
    nohup java $JAVA_OPT $SYS_OPT -Dspring.profiles.active=$ENV  -cp ../conf:../lib/* com.sumscope.wf.bond.monitor.BondMonitorApplication  &> nohup.out &


    curr_dir=`pwd`
    fails=0
    while [ $fails -le 3 ]; do
        for pid in `pgrep java`; do
            one_dir=`readlink -e /proc/$pid/cwd`
            if [ "$one_dir" != "" ] && [ "$one_dir" == "$curr_dir" ]; then
                echo $pid':' $one_dir
                exit 0
            fi
        done
        sleep 1
        fails=$(($fails + 1))
    done
    echo 'open error...'
    tail -n 15 nohup.out

    cd $start_dir
fi