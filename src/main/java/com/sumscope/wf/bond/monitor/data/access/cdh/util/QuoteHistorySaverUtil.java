package com.sumscope.wf.bond.monitor.data.access.cdh.util;

import com.sumscope.wf.bond.monitor.data.model.db.QuoteHistories;
import com.sumscope.wf.bond.monitor.data.model.db.QuoteHistoryOriginals;
import com.sumscope.wf.bond.monitor.data.model.db.QuoteHistoryStats;
import com.sumscope.wf.bond.monitor.data.model.repository.QuoteHistoriesRepo;
import com.sumscope.wf.bond.monitor.data.model.repository.QuoteHistoryOriginalsRepo;
import com.sumscope.wf.bond.monitor.data.model.repository.QuoteHistoryStatsRepo;
import com.sumscope.wf.bond.monitor.global.enums.SaveDBTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

@Component
public class QuoteHistorySaverUtil {
    private static final Logger logger = LoggerFactory.getLogger(QuoteHistorySaverUtil.class);

    @Autowired
    private JdbcUtil jdbcUtil;
    @Autowired
    private QuoteHistoriesRepo quoteHistoriesRepo;
    @Autowired
    private QuoteHistoryStatsRepo quoteHistoryStatsRepo;
    @Autowired
    private QuoteHistoryOriginalsRepo quoteHistoryOriginalsRepo;

    public void saveQHO(List<QuoteHistoryOriginals> list, SaveDBTypeEnum typeEnum){
        if(CollectionUtils.isEmpty(list)) return;

        if(SaveDBTypeEnum.JDBC.equal(typeEnum)){
            List<String> sqlList = convertToSqlQHO(list);
            jdbcUtil.save(sqlList);
        } else {

        }
    }

    public void saveQH(List<QuoteHistories> list,SaveDBTypeEnum typeEnum){
        if(CollectionUtils.isEmpty(list)) return;

        if(SaveDBTypeEnum.JDBC.equal(typeEnum)){
            List<String> sqlList = convertToSqlQH(list);
            jdbcUtil.save(sqlList);
        } else {

        }
    }

    public void saveQHS(List<QuoteHistoryStats> list,SaveDBTypeEnum typeEnum){
        if(CollectionUtils.isEmpty(list)) return;

        if(SaveDBTypeEnum.JDBC.equal(typeEnum)){
            List<String> sqlList = convertToSqlQHS(list);
            jdbcUtil.save(sqlList);
        } else {

        }
    }

    public void deleteQhAndQho(String date){
        quoteHistoriesRepo.deleteByDate(date);
        quoteHistoryOriginalsRepo.deleteByDate(date);
    }

    private List<String> convertToSqlQHO(List<QuoteHistoryOriginals> dataList){
        List<String> list = new ArrayList<>();
        if(CollectionUtils.isEmpty(dataList)) return list;
        dataList.forEach(bi -> {
            String insertSql = String.format("INSERT INTO `quote_history_originals` (" +
                    "`quote_time`,`quote_type`,`bond_key`,`listed_market`,`remain_time`," +
                    "`remain_time_value`,`id`,`trans_type`) VALUES" +
                    "(%s,%s,%s,%s,%s,%s,%s,%s)",
                    bi.getQuoteTime() == null ? null : "'"+bi.getQuoteTime()+"'",
                    bi.getQuoteType() == null ? null : "'"+bi.getQuoteType()+"'",
                    bi.getBondKey() == null ? null : "'"+bi.getBondKey()+"'",
                    bi.getListedMarket() == null ? null : "'"+bi.getListedMarket()+"'",
                    bi.getRemainTime() == null ? null : "'"+bi.getRemainTime()+"'",
                    bi.getRemainTimeValue() == null ? null : "'"+bi.getRemainTimeValue()+"'",
                    bi.getId() == null ? null : "'"+bi.getId()+"'",
                    bi.getTransType() == null ? null : "'"+bi.getTransType()+"'"
                    );
            list.add(insertSql);
        });
        return list;
    }

    private List<String> convertToSqlQH(List<QuoteHistories> dataList){
        List<String> list = new ArrayList<>();
        if(CollectionUtils.isEmpty(dataList)) return list;
        dataList.forEach(bi -> {
            String insertSql = String.format("INSERT INTO `quote_histories` (" +
                            "`quote_time`,`quote_type`,`bond_key`,`listed_market`,`remain_time`," +
                            "`remain_time_value`) VALUES" +
                            "(%s,%s,%s,%s,%s,%s)",
                    bi.getQuoteTime() == null ? null : "'"+bi.getQuoteTime()+"'",
                    bi.getQuoteType() == null ? null : "'"+bi.getQuoteType()+"'",
                    bi.getBondKey() == null ? null : "'"+bi.getBondKey()+"'",
                    bi.getListedMarket() == null ? null : "'"+bi.getListedMarket()+"'",
                    bi.getRemainTime() == null ? null : "'"+bi.getRemainTime()+"'",
                    bi.getRemainTimeValue() == null ? null : "'"+bi.getRemainTimeValue()+"'"
            );
            list.add(insertSql);
        });
        return list;
    }

    private List<String> convertToSqlQHS(List<QuoteHistoryStats> dataList){
        List<String> list = new ArrayList<>();
        if(CollectionUtils.isEmpty(dataList)) return list;
        dataList.forEach(bi ->{
            String insertSql = String.format("INSERT INTO `quote_history_stats` " +
                    "(`start_time`,`quote_type`,`total`) VALUES(%s,%s,%s)",
                    bi.getStartTime() == null ? null : "'"+bi.getStartTime()+"'",
                    bi.getQuoteType(),
                    bi.getTotal() == null ? null : "'"+bi.getTotal()+"'"
            );
            list.add(insertSql);
        });
        return list;
    }

    public boolean isSameWeek(String date){
        Calendar c = Calendar.getInstance();
        int currentWeek = c.get(Calendar.WEEK_OF_YEAR);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date parse = sdf.parse(date);
            c.setTime(parse);
            int week = c.get(Calendar.WEEK_OF_YEAR);
            if(currentWeek == week) return true;
            return false;
        } catch (Exception e) {
            logger.error("isSameWeek fun error.  date={}   exception={}",date,e.getMessage());
            return false;
        }
    }
}
