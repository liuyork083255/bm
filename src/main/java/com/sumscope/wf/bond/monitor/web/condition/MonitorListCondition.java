package com.sumscope.wf.bond.monitor.web.condition;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.hibernate.validator.constraints.NotBlank;

import javax.validation.constraints.Pattern;

@ApiModel(value = "获取中债估值监控列表信息条件")
public class MonitorListCondition {
    @ApiModelProperty(value = "中债基准Code", position = 1)
    @NotBlank(message = "curveCode can not be null")
    private String curveCode;
    @ApiModelProperty(value = "偏离值", position = 2)
    @NotBlank(message = "diff can not be null")
    @Pattern(regexp = "5|10|15|20|25", message = "参数diff取值范围 5|10|15|20|25")
    private String diff;
    @ApiModelProperty(value = "排序字段[short_name, cdc_price, latest_price, avg_diff_price, price_diff_count, latest_price_diff, trade_count, update_time]", position = 3)
    @NotBlank(message = "orderBy can not be null")
    @Pattern(regexp = "short_name|cdc_price|latest_price|avg_diff_price|price_diff_count|latest_price_diff|trade_count|update_time", message = "不符合排序字段列表")
    private String orderBy;
    @ApiModelProperty(value = "排序(desc, asc)", position = 4)
    @Pattern(regexp = "desc|asc", message = "参数diff取值范围 desc|asc")
    @NotBlank(message = "sort can not be null")
    private String sort;
    @ApiModelProperty(value = "偏移值", position = 5)
    private Integer offset;
    @ApiModelProperty(value = "每页值", position = 6)
    private Integer limit;

    public String getCurveCode() {
        return curveCode;
    }

    public void setCurveCode(String curveCode) {
        this.curveCode = curveCode;
    }

    public String getDiff() {
        return diff;
    }

    public void setDiff(String diff) {
        this.diff = diff;
    }

    public String getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(String orderBy) {
        this.orderBy = orderBy;
    }

    public Integer getOffset() {
        return offset;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public String getSort() {
        return sort;
    }

    public void setSort(String sort) {
        this.sort = sort;
    }
}
