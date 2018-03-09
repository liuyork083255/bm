package com.sumscope.wf.bond.monitor.scheduled;

import com.sumscope.wf.bond.monitor.data.access.cdh.CdhBeforeWorkdayLoader;
import com.sumscope.wf.bond.monitor.data.access.cdh.util.BondInfoLoaderUtil;
import com.sumscope.wf.bond.monitor.data.access.cdh.CdhYieldCurveLoader;
import com.sumscope.wf.bond.monitor.data.access.cdh.util.InitMain;
import com.sumscope.wf.bond.monitor.data.access.cdh.util.UserAttentionUtil;
import com.sumscope.wf.bond.monitor.data.model.repository.TradeStatsRepo;
import com.sumscope.wf.bond.monitor.data.model.repository.TradesRepo;
import com.sumscope.wf.bond.monitor.global.ConfigParams;
import com.sumscope.wf.bond.monitor.global.constants.BondMonitorConstants;
import com.sumscope.wf.bond.monitor.utils.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.List;

@Component
public class BondMonitorScheduled {
    private static final Logger logger = LoggerFactory.getLogger(BondMonitorScheduled.class);
    @Autowired
    private InitMain initMain;
    @Autowired
    private TradesRepo tradesRepo;
    @Autowired
    private BondInfoLoaderUtil bondInfoLoaderUtil;
    @Autowired
    private TradeStatsRepo tradeStatsRepo;
    @Autowired
    private UserAttentionUtil userAttentionUtil;
    @Autowired
    private CdhYieldCurveLoader cdhYieldCurveLoader;
    @Autowired
    private CdhBeforeWorkdayLoader cdhBeforeWorkdayLoader;
    @Autowired
    private ConfigParams configParams;

    @Scheduled(cron = "${application.scheduled.flush.bond.info}")
    private void flushBondInfoTableYieldCurveIdField(){
        bondInfoLoaderUtil.updateBondInfoTableByDay();
    }

    @Scheduled(cron = "${application.scheduled.clear.yieldCurveCache}")
    private void clearYieldCurveCache(){
        cdhYieldCurveLoader.clearYieldCurveCache();
    }

    /**
     * 每晚定时清空 trades trades_stats 两张表
     */
    @Scheduled(cron = "${application.scheduled.clear.tables}")
    private void clearTables(){
        logger.info("clear tables start ...");
        try {
            tradesRepo.deleteAll();
            tradeStatsRepo.deleteAll();
        } catch (Exception e) {
            logger.error("clear tables error.    exception:{}",e.getMessage());
        }
        logger.info("clear tables done");
    }

    /**
     * 开始加载 quote_history_originals quote_histories quote_history_stats
     */
    @Scheduled(cron = "${application.scheduled.load.history.tables}")
    public void loadQuoteHistory(){

        // 获取
        Integer loadQuoteHistoryDays = configParams.getLoadQuoteHistoryDays();
        if(loadQuoteHistoryDays < 1) return;

        List<String> preWorkdayList = cdhBeforeWorkdayLoader.getPreWorkdayList(loadQuoteHistoryDays);

        // 判断当天是否为工作日
        if(!preWorkdayList.contains(DateUtils.formatter(LocalDate.now(), BondMonitorConstants.DATE_YYYYMMDD))){
            logger.info("current {} is not workday",LocalDate.now());
            return;
        }

        String findDate;
        String deleteDate;
        try {
            // [20180301, 20180302, 20180305, 20180306, 20180307] 20180301为要删除日，20180307为要加载日
            findDate = DateUtils.parseStringDate(preWorkdayList.get(loadQuoteHistoryDays - 1));
            deleteDate = DateUtils.parseStringDate(preWorkdayList.get(0));
        } catch (Exception e) {
            logger.error("convert date error.   exception={}",e.getMessage());
            return;
        }
        initMain.deleteQuoteHistoryLastDay(deleteDate); // 删除 quote_history_originals quote_histories 最早的数据
        initMain.loadQuoteHistory(findDate);            // 插入 quote_history_originals quote_histories 当天数据
        initMain.loadQuoteHistoryStats();               // 刷新 quote_history_stats 统计数据
    }



}
