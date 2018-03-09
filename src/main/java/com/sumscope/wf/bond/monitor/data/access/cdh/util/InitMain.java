package com.sumscope.wf.bond.monitor.data.access.cdh.util;

import com.sumscope.wf.bond.monitor.data.access.cdh.CdhBeforeWorkdayLoader;
import com.sumscope.wf.bond.monitor.data.access.cdh.CdhBondMarketStreamLoader;
import com.sumscope.wf.bond.monitor.data.access.cdh.CdhBondTradeLoader;
import com.sumscope.wf.bond.monitor.data.model.db.QuoteHistories;
import com.sumscope.wf.bond.monitor.data.model.db.QuoteHistoryOriginals;
import com.sumscope.wf.bond.monitor.data.model.db.QuoteHistoryStats;
import com.sumscope.wf.bond.monitor.data.model.repository.BondInfosRepo;
import com.sumscope.wf.bond.monitor.data.model.repository.QuoteHistoriesRepo;
import com.sumscope.wf.bond.monitor.data.model.repository.QuoteHistoryStatsRepo;
import com.sumscope.wf.bond.monitor.global.ConfigParams;
import com.sumscope.wf.bond.monitor.global.constants.BondMonitorConstants;
import com.sumscope.wf.bond.monitor.global.enums.SaveDBTypeEnum;
import com.sumscope.wf.bond.monitor.utils.DateUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * 该类主要负责控制程序初始化启动加载数据
 * 严格控制加载数据顺序和时间
 */
@Component
public class InitMain {
    private static final Logger logger = LoggerFactory.getLogger(InitMain.class);

    @Autowired
    private ConfigParams configParams;
    @Autowired
    private QuoteHistorySaverUtil quoteHistorySaverUtil;
    @Autowired
    private BondInfoLoaderUtil bondInfoLoaderUtil;
    @Autowired
    private BondInfosRepo bondInfosRepo;
    @Autowired
    private CdhBondMarketStreamLoader cdhBondMarketStreamLoader;
    @Autowired
    private CdhBeforeWorkdayLoader cdhBeforeWorkdayLoader;
    @Autowired
    private CdhBondTradeLoader cdhBondTradeLoader;
    @Autowired
    private QuoteHistoryStatsUtil quoteHistoryStatsUtil;
    @Autowired
    private QuoteHistoriesRepo quoteHistoriesRepo;
    @Autowired
    private QuoteHistoryStatsRepo quoteHistoryStatsRepo;

    @PostConstruct
    private void init(){
        Boolean isLoadBondInfo = configParams.getLoadBondInfo();
        if(isLoadBondInfo){
            logger.info("load bond_infos data starting ...");
            loadBondinfo();
        }

        Integer loadQuoteHistoryDays = configParams.getLoadQuoteHistoryDays();
        if(loadQuoteHistoryDays > 0){
            logger.info("load quote_history {} days data starting ...",loadQuoteHistoryDays);
            // 获取前 loadQuoteHistoryDays 个工作日
            List<String> preWorkdayList = cdhBeforeWorkdayLoader.getPreWorkdayList(loadQuoteHistoryDays);
            String current = LocalDate.now().format(DateTimeFormatter.ofPattern(BondMonitorConstants.DATE_YYYYMMDD));
            preWorkdayList.forEach(date ->{
                String dateFmt = DateUtils.parseStringDate(date);
                // 防止上线当天出现重复加载问题，上线当天如果是工作日，则晚上定时任务会执行
                if(StringUtils.equalsIgnoreCase(current,dateFmt)) return;
                loadQuoteHistory(dateFmt);
            });
        }
        // 确保在 loadQuoteHistory 方法后面执行
        loadQuoteHistoryStats();
    }

    /**
     * 加载 quote_history_originals quote_histories
     * 加载顺序：
     *      quote_history_originals
     *          该表数据来源 marketStream 和 trade
     *      quote_histories
     *          该表数据来源 quote_history_originals 表，并且当中的 trade 行情需要处理 delete update 逻辑
     *
     * @param date 加载日期
     */
    public void loadQuoteHistory(String date){
        // 加载报价 QuoteHistoryOriginals
        List<QuoteHistoryOriginals> streamListQHO = cdhBondMarketStreamLoader.getQHOList(date);
        // 加载成交 QuoteHistoryOriginals
        List<QuoteHistoryOriginals> tradeListQHO = cdhBondTradeLoader.getQHOList(date);


        // 报价 QuoteHistoryOriginals 转为 QuoteHistories
        List<QuoteHistories> streamListQH = cdhBondMarketStreamLoader.getQHList(streamListQHO);
        // 成交 QuoteHistoryOriginals 转为 QuoteHistories
        List<QuoteHistories> tradeListQH = cdhBondTradeLoader.getQHList(tradeListQHO);


        streamListQHO.addAll(tradeListQHO);
        insertQuoteHistoryOriginals(streamListQHO);

        streamListQH.addAll(tradeListQH);
        insertQuoteHistories(streamListQH);
    }

    /**
     * 加载 quote_history_stats
     * 该方法调用必须在 quote_history_originals 和 quote_histories 加载完成后调用，否则统计有误
     * 并且在插入前需要清空表
     */
    public void loadQuoteHistoryStats(){
        quoteHistoryStatsRepo.deleteAll();
        // 获取所有 QuoteHistories
        List<QuoteHistories> qhList = quoteHistoriesRepo.findAll();
        // 转换 QuoteHistories 为QuoteHistoryStats
        List<QuoteHistoryStats> qhsList = quoteHistoryStatsUtil.convertToQuoteHistoryStats(qhList);
        insertQuoteHistoryStats(qhsList);
    }

    /**
     * bond_infos
     */
    private void loadBondinfo(){
        bondInfosRepo.deleteAll0();
        bondInfoLoaderUtil.loadBondInfoFirstStart();
    }

    private void insertQuoteHistoryOriginals(List<QuoteHistoryOriginals> list){
        quoteHistorySaverUtil.saveQHO(list, SaveDBTypeEnum.JDBC);
    }

    private void insertQuoteHistories(List<QuoteHistories> list){
        quoteHistorySaverUtil.saveQH(list,SaveDBTypeEnum.JDBC);
    }

    private void insertQuoteHistoryStats(List<QuoteHistoryStats> list){
        quoteHistorySaverUtil.saveQHS(list,SaveDBTypeEnum.JDBC);
    }

    public void deleteQuoteHistoryLastDay(String date){
        quoteHistorySaverUtil.deleteQhAndQho(date);
    }
}
