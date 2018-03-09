package com.sumscope.wf.bond.monitor.data.model.cache;

import java.util.List;

public class YieldCurveModel {
    private List<String> period;
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
