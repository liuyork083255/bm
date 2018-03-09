package com.sumscope.wf.bond.monitor.domain.monitor;

import io.swagger.annotations.ApiModel;

import java.io.Serializable;

@ApiModel(value = "处理前台警告类型请求返回体")
public class WarnDTO implements Serializable {
    public WarnDTO(String msg){
        this.message = msg;
    }
    private String message;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
