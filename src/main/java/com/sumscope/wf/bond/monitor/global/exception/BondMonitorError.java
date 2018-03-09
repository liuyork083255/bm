package com.sumscope.wf.bond.monitor.global.exception;

public class BondMonitorError extends RuntimeException {
    private int code;
    private String msg;
    BondMonitorError(int code, String msg){
        super(msg);
        this.msg = msg;
        this.code = code;
    }

    public static final BondMonitorError InitFailed = new BondMonitorError(10001, "系统初始化失败");

    public static final BondMonitorError SaveDBFailed = new BondMonitorError(5001, "数据库保存失败");
    public static final BondMonitorError DeleteDBFailed = new BondMonitorError(50002, "数据库删除失败");
    public static final BondMonitorError FindDBFailed = new BondMonitorError(50003, "数据库查询失败");


    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
