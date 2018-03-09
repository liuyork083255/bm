package com.sumscope.wf.bond.monitor.service.monitor;

import com.sumscope.wf.bond.monitor.data.model.db.YieldCurves;
import com.sumscope.wf.bond.monitor.domain.monitor.*;
import com.sumscope.wf.bond.monitor.web.condition.CdcCurveCondition;
import com.sumscope.wf.bond.monitor.web.condition.MonitorListCondition;
import com.sumscope.wf.bond.monitor.web.condition.MonitorPlotCondition;

import java.util.List;

public interface BondMonitorServiceI {
    List<String> getDiffValue();

    List<YieldCurves> getCurveInfo();

    CdcCurveDTO getCdcCurve(String curveCode);

    List<MonitorPlotDTO> getMonitorPlot(MonitorPlotCondition condition);

    MonitorListDTO getMonitorList(MonitorListCondition condition);

    BondInfoDTO getBondInfoByCode(String bondCode, String diff, String userId);
}
