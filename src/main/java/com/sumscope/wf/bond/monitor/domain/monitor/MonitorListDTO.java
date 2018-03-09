package com.sumscope.wf.bond.monitor.domain.monitor;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.List;

@ApiModel(value = "中债估值监控信息")
public class MonitorListDTO {
    @ApiModelProperty(value = "总数", position = 1)
    private int total;
    @ApiModelProperty(value = "监控列表信息", position = 2)
    private List<MonitorListItemDTO> data;

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public List<MonitorListItemDTO> getData() {
        return data;
    }

    public void setData(List<MonitorListItemDTO> data) {
        this.data = data;
    }
}
