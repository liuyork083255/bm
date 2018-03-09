package com.sumscope.wf.bond.monitor.web.condition;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.hibernate.validator.constraints.NotBlank;

import java.math.BigDecimal;

@ApiModel(value = "获取中债估值监控曲线信息条件")
public class MonitorPlotCondition {
    @ApiModelProperty(value = "中债基准Code", position = 1)
    @NotBlank(message = "curveCode can not be null")
    private String curveCode;
    @ApiModelProperty(value = "偏离值", position = 2)
    private BigDecimal diff;
    @ApiModelProperty(value = "用户id", position = 3)
    @NotBlank(message = "userId can not be null")
    private String userId;

    public String getCurveCode() {
        return curveCode;
    }

    public void setCurveCode(String curveCode) {
        this.curveCode = curveCode;
    }

    public BigDecimal getDiff() {
        return diff;
    }

    public void setDiff(BigDecimal diff) {
        this.diff = diff;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }
}
