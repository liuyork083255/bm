package com.sumscope.wf.bond.monitor.domain.monitor;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.math.BigDecimal;

@ApiModel(value = "中债估值监控信息")
public class MonitorListItemDTO {
    @ApiModelProperty(value = "债券Id", position = 1)
    private String bondCode;
    @ApiModelProperty(value = "中债估值", position = 2)
    private BigDecimal cdcPrice;
    @ApiModelProperty(value = "偏离值", position = 3)
    private BigDecimal curDiff;
    @ApiModelProperty(value = "最新价格", position = 4)
    private BigDecimal latestPrice;
    @ApiModelProperty(value = "债券简称", position = 5)
    private String shortName;
    @ApiModelProperty(value = "当日估值偏离", position = 6)
    private BigDecimal todayAvgDiff;
    @ApiModelProperty(value = "今日成交笔数", position = 7)
    private Integer todayDealNum;
    @ApiModelProperty(value = "当日偏离次数", position = 8)
    private Integer todayDiffNum;
    @ApiModelProperty(value = "最后修改时间", position = 9)
    private String updateTime;

    public String getBondCode() {
        return bondCode;
    }

    public void setBondCode(String bondCode) {
        this.bondCode = bondCode;
    }

    public BigDecimal getCdcPrice() {
        return cdcPrice;
    }

    public void setCdcPrice(BigDecimal cdcPrice) {
        this.cdcPrice = cdcPrice;
    }

    public BigDecimal getCurDiff() {
        return curDiff;
    }

    public void setCurDiff(BigDecimal curDiff) {
        this.curDiff = curDiff;
    }

    public BigDecimal getLatestPrice() {
        return latestPrice;
    }

    public void setLatestPrice(BigDecimal latestPrice) {
        this.latestPrice = latestPrice;
    }

    public String getShortName() {
        return shortName;
    }

    public void setShortName(String shortName) {
        this.shortName = shortName;
    }

    public BigDecimal getTodayAvgDiff() {
        return todayAvgDiff;
    }

    public void setTodayAvgDiff(BigDecimal todayAvgDiff) {
        this.todayAvgDiff = todayAvgDiff;
    }

    public Integer getTodayDealNum() {
        return todayDealNum;
    }

    public void setTodayDealNum(Integer todayDealNum) {
        this.todayDealNum = todayDealNum;
    }

    public Integer getTodayDiffNum() {
        return todayDiffNum;
    }

    public void setTodayDiffNum(Integer todayDiffNum) {
        this.todayDiffNum = todayDiffNum;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }
}
