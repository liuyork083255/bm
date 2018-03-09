#!/bin/bash

PROCESSNUM=`ps -ef | grep com.sumscope.wf.bond.monitor.BondMonitorApplication  | grep -v grep | wc -l`

if [ $PROCESSNUM -gt 0 ]
then
    kill -s 15 `ps -ef | grep com.sumscope.wf.bond.monitor.BondMonitorApplication  | grep -v grep | awk '{print $2}'`
    echo "process stop success"
else
    echo "process is not running..."
fi