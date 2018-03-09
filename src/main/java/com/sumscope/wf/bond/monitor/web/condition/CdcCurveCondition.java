package com.sumscope.wf.bond.monitor.web.condition;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.hibernate.validator.constraints.NotBlank;

@ApiModel(value = "中债基准曲线图表中的线")
public class CdcCurveCondition {
    @ApiModelProperty(value = "中债基准Code", position = 1)
    @NotBlank(message = "curveCode can not be null")
    private String curveCode;

    public String getCurveCode() {
        return curveCode;
    }

    public void setCurveCode(String curveCode) {
        this.curveCode = curveCode;
    }
}
