package com.sumscope.wf.bond.monitor.domain.monitor;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.List;

@ApiModel(value = "图表")
public class RangeDTO {
    @ApiModelProperty(value = "近一周数据", position = 1)
    private List<String> lastWeek;
    @ApiModelProperty(value = "时间", position = 2)
    private List<String> time;
    @ApiModelProperty(value = "今日数据", position = 3)
    private List<String> today;

    public List<String> getLastWeek() {
        return lastWeek;
    }

    public void setLastWeek(List<String> lastWeek) {
        this.lastWeek = lastWeek;
    }

    public List<String> getTime() {
        return time;
    }

    public void setTime(List<String> time) {
        this.time = time;
    }

    public List<String> getToday() {
        return today;
    }

    public void setToday(List<String> today) {
        this.today = today;
    }
}
