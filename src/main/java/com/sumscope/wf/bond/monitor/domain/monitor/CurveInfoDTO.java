package com.sumscope.wf.bond.monitor.domain.monitor;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.HashMap;
import java.util.Map;

@ApiModel(value = "样券曲线名称")
public class CurveInfoDTO {
    @ApiModelProperty(value = "曲线名称值",position = 1)
    private Map<String,String> curveInfoList;

    public Map<String, String> getCurveInfoList() {
        return curveInfoList;
    }

    public void setCurveInfoList(Map<String, String> curveInfoList) {
        this.curveInfoList = curveInfoList;
    }
}
