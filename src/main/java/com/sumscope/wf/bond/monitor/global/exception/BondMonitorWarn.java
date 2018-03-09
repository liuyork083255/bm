package com.sumscope.wf.bond.monitor.global.exception;

public class BondMonitorWarn extends RuntimeException {
    private BondMonitorWarn(String msg){
        super(msg);
    }

    public static BondMonitorWarn warn(String msg){
        return new BondMonitorWarn(msg);
    }
}
