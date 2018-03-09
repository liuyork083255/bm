package com.sumscope.wf.bond.monitor.web.controller;

import com.sumscope.service.webbond.common.web.response.ResponseData;
import com.sumscope.service.webbond.common.web.response.ResponseUtil;
import com.sumscope.wf.bond.monitor.data.model.db.YieldCurves;
import com.sumscope.wf.bond.monitor.domain.monitor.BondInfoDTO;
import com.sumscope.wf.bond.monitor.domain.monitor.CdcCurveDTO;
import com.sumscope.wf.bond.monitor.domain.monitor.MonitorListDTO;
import com.sumscope.wf.bond.monitor.domain.monitor.MonitorPlotDTO;
import com.sumscope.wf.bond.monitor.service.monitor.BondMonitorServiceI;
import com.sumscope.wf.bond.monitor.service.monitor.BondMonitorServiceImpl;
import com.sumscope.wf.bond.monitor.web.condition.MonitorListCondition;
import com.sumscope.wf.bond.monitor.web.condition.MonitorPlotCondition;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.List;

@Api(tags = "Bond Monitor 相关API")
@Validated
@RestController
@RequestMapping(value = "${server.servlet.path.v1.prefix}", produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
public class BondMonitorController {

    @Autowired
    private BondMonitorServiceI bondMonitorServiceI;
    @Autowired
    private BondMonitorServiceImpl bondMonitorService;

    @ApiOperation(value = "获取偏离值的取值列表", notes = "获取偏离值的取值列表")
    @RequestMapping(value = "/diffValue", method = RequestMethod.GET)
    public ResponseData<List<String>> diffValue() {
        return ResponseUtil.ok(bondMonitorServiceI.getDiffValue());
    }

    @ApiOperation(value = "获取曲线名称", notes = "获取曲线名称表")
    @RequestMapping(value = "/curveInfo", method = RequestMethod.GET)
    public ResponseData<List<YieldCurves>> curveInfo() {
        return ResponseUtil.ok(bondMonitorServiceI.getCurveInfo());
    }

    @ApiOperation(value = "获取中债基准曲线图表中的线", notes = "获取中债基准曲线图表中的线")
    @RequestMapping(value = "/cdcCurve", method = RequestMethod.GET)
    public ResponseData<CdcCurveDTO> cdcCurve(
            @NotBlank(message = "curveCode 不能为空")
            @RequestParam(name = "curveCode", required = false) String curveCode) {
        return ResponseUtil.ok(bondMonitorServiceI.getCdcCurve(curveCode));
    }

    @ApiOperation(value = "获取中债估值监控曲线信息", notes = "获取中债估值监控曲线信息(偏离点信息)")
    @RequestMapping(value = "/monitorPlot", method = RequestMethod.POST)
    public ResponseData<List<MonitorPlotDTO>> monitorPlot(
            @ApiParam(value = "中债估值参数", required = true)
            @Valid @RequestBody
                    MonitorPlotCondition monitorPlotCondition) {
        return ResponseUtil.ok(bondMonitorService.getMonitorPlot(monitorPlotCondition));
    }

    @ApiOperation(value = "获取中债估值监控列表信息", notes = "获取中债估值监控列表信息")
    @RequestMapping(value = "/monitorList", method = RequestMethod.POST)
    public ResponseData<MonitorListDTO> monitorList(@ApiParam(value = "中债估值参数", required = true)
                                                    @Valid @RequestBody
                                                            MonitorListCondition monitorListCondition) {
        return ResponseUtil.ok(bondMonitorService.getMonitorList(monitorListCondition));
    }

    @ApiOperation(value = "根据债券代码获取中债估值监控列表信息", notes = "根据债券代码获取中债估值监控列表信息")
    @RequestMapping(value = "/bondInfo", method = RequestMethod.GET)
    public ResponseData<BondInfoDTO> bondInfo(@ApiParam(value = "债券代码", required = true)
                                              @NotBlank(message = "债券代码 不能为空")
                                              @RequestParam(name = "bondCode", required = false) String bondCode,
                                              @ApiParam(value = "偏离值")
                                              @RequestParam(required = false) String diff,
                                              @ApiParam(value = "用户id", required = true)
                                              @NotBlank(message = "用户id不能为空")
                                              @RequestParam(name = "userId", required = false) String userId) {
        return ResponseUtil.ok(bondMonitorService.getBondInfoByCode(bondCode, diff, userId));
    }
}