package com.sumscope.wf.bond.monitor.domain.monitor;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.math.BigDecimal;

@ApiModel(value = "中债基准曲线图表中的点")
public class MonitorPlotDTO {
    @ApiModelProperty(value = "债券代码", position = 1)
    private String bondCode;
    @ApiModelProperty(value = "差值", position = 2)
    private BigDecimal diff;
    @ApiModelProperty(value = "是否关注", position = 3)
    private Boolean isFollow;
    @ApiModelProperty(value = "债券名称", position = 4)
    private String name;
    @ApiModelProperty(value = "债券价格", position = 5)
    private BigDecimal price;
    @ApiModelProperty(value = "剩余期限", position = 6)
    private String term;
    @ApiModelProperty(value = "时间", position = 7)
    private String time;
    @ApiModelProperty(value = "中债曲线code", position = 8)
    private String yieldCurve;

    public String getBondCode() {
        return bondCode;
    }

    public void setBondCode(String bondCode) {
        this.bondCode = bondCode;
    }

    public BigDecimal getDiff() {
        return diff;
    }

    public void setDiff(BigDecimal diff) {
        this.diff = diff;
    }

    public Boolean getIsFollow() {
        return isFollow;
    }

    public void setIsFollow(Boolean follow) {
        isFollow = follow;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    public String getTerm() {
        return term;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getYieldCurve() {
        return yieldCurve;
    }

    public void setYieldCurve(String yieldCurve) {
        this.yieldCurve = yieldCurve;
    }
}
