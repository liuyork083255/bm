package com.sumscope.wf.bond.monitor.web.controller;

import com.sumscope.wf.bond.monitor.data.access.cdh.CdhBeforeWorkdayLoader;
import com.sumscope.wf.bond.monitor.data.access.cdh.CdhBondTradeLoader;
import com.sumscope.wf.bond.monitor.data.access.cdh.util.BondInfoLoaderUtil;
import com.sumscope.wf.bond.monitor.data.access.cdh.util.InitMain;
import com.sumscope.wf.bond.monitor.data.access.cdh.util.QuoteHistoryStatsUtil;
import com.sumscope.wf.bond.monitor.data.model.repository.QuoteHistoriesRepo;
import com.sumscope.wf.bond.monitor.data.model.repository.QuoteHistoryStatsRepo;
import com.sumscope.wf.bond.monitor.scheduled.BondMonitorScheduled;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Api(tags = "Bond Realtime 相关API")
@Validated
@RestController
@RequestMapping(value = "${server.servlet.path.v2.prefix}", produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
public class BondRealtimeController {

    @Autowired
    private InitMain initMain;
    @Autowired
    private CdhBeforeWorkdayLoader cdhBeforeWorkdayLoader;
    @Autowired
    private CdhBondTradeLoader cdhBondTradeLoader;
    @Autowired
    private QuoteHistoryStatsRepo quoteHistoryStatsRepo;
    @Autowired
    private QuoteHistoryStatsUtil quoteHistoryStatsUtil;
    @Autowired
    private QuoteHistoriesRepo quoteHistoriesRepo;
    @Autowired
    private BondMonitorScheduled bondMonitorScheduled;
    @Autowired
    private BondInfoLoaderUtil bondInfoLoaderUtil;

    @RequestMapping(value = "/t1", method = RequestMethod.GET)
    public Object myT1(@ApiParam(value = "day", required = true) @RequestParam int day) {
        String preWorkday = cdhBeforeWorkdayLoader.getPreWorkday(day);
        List<String> preWorkdayList = cdhBeforeWorkdayLoader.getPreWorkdayList(day);
        System.out.println("preWorkday is:" + preWorkday);
        System.out.println("preWorkdayList is:" + preWorkdayList);
        return null;
    }

    @RequestMapping(value = "/t2", method = RequestMethod.GET)
    public Object myT2() {
        bondInfoLoaderUtil.updateBondInfoTableByDay();
        return null;
    }

    @RequestMapping(value = "/t3", method = RequestMethod.GET)
    public Object myT3(@ApiParam(value = "date", required = true)@RequestParam String date) {
        initMain.loadQuoteHistory(date);
        return null;
    }

    @RequestMapping(value = "/t4", method = RequestMethod.GET)
    public Object myT4(@ApiParam(value = "date", required = true)@RequestParam String date) {
        quoteHistoriesRepo.deleteByDate(date);
        return null;
    }
}
