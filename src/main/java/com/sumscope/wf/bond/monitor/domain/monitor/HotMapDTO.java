package com.sumscope.wf.bond.monitor.domain.monitor;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.List;

@ApiModel(value = "热力图")
public class HotMapDTO {
    @ApiModelProperty(value = "请求的类型", position = 1)
    private String type;
    @ApiModelProperty(value = "X轴标题", position = 2)
    private List<String> x;
    @ApiModelProperty(value = "Y轴标题", position = 3)
    private List<String> y;
    @ApiModelProperty(value = "没有折行的所有数据", position = 4)
    private List<Integer> data;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<String> getX() {
        return x;
    }

    public void setX(List<String> x) {
        this.x = x;
    }

    public List<String> getY() {
        return y;
    }

    public void setY(List<String> y) {
        this.y = y;
    }

    public List<Integer> getData() {
        return data;
    }

    public void setData(List<Integer> data) {
        this.data = data;
    }
}
