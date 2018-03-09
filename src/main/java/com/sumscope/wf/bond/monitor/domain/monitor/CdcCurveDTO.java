package com.sumscope.wf.bond.monitor.domain.monitor;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.List;

@ApiModel(value = "中债基准曲线图表中线的返回体")
public class CdcCurveDTO {
    @ApiModelProperty(value = "期限", position = 1)
    private List<String> period;
    @ApiModelProperty(value = "估值", position = 2)
    private List<String> yield;

    public List<String> getPeriod() {
        return period;
    }

    public void setPeriod(List<String> period) {
        this.period = period;
    }

    public List<String> getYield() {
        return yield;
    }

    public void setYield(List<String> yield) {
        this.yield = yield;
    }
}
