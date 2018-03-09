package com.sumscope.wf.bond.monitor.data.access.cdh;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sumscope.wf.bond.monitor.data.access.cdh.util.QuoteHistorySaverUtil;
import com.sumscope.wf.bond.monitor.data.model.db.QuoteHistories;
import com.sumscope.wf.bond.monitor.data.model.db.QuoteHistoryOriginals;
import com.sumscope.wf.bond.monitor.global.ConfigParams;
import com.sumscope.wf.bond.monitor.global.constants.BondMonitorConstants;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

@Component
public class CdhBondTradeLoader {
    private static final Logger logger = LoggerFactory.getLogger(CdhBondTradeLoader.class);
    @Autowired
    private ConfigParams configParams;
    @Autowired
    private CdhRestWrapper cdhRestWrapper;
    @Autowired
    private QuoteHistorySaverUtil quoteHistorySaverUtil;

    private List<JSONObject> loadBondTradeWebbondFmt(String date,String apiName){
        List<JSONObject> jsonObjectList = cdhRestWrapper.getJSONObjectList(
                configParams.getRestfulUrl(),
                configParams.getUsername(),
                configParams.getPassword(),
                BondMonitorConstants.RESTFUL_PAGE_SIZE,
                BondMonitorConstants.RESTFUL_DATASOURCE_ID_106,
                configParams.getBondTradeWebbondFmt(),
                String.format("updateDateTime <= '%s 23:59:59' and updateDateTime >= '%s 00:00:00'",date,date),
                BondMonitorConstants.RESTFUL_BOND_TRADE_WEBBOND_FMT);
        logger.info("load bond_trade_webbond_fmt size = {}",jsonObjectList.size());
        return jsonObjectList;
    }

    public List<QuoteHistoryOriginals> getQHOList(String date){
        logger.info("load bond_trade_webbond_fmt stating by={}",date);

        List<QuoteHistoryOriginals> dbList = new ArrayList<>();

        List<JSONObject> jsonObjects;
        // 判断当前日期是否为本周，否则需要从历史表获取数据
        if(quoteHistorySaverUtil.isSameWeek(date)){
            jsonObjects = loadBondTradeWebbondFmt(date,configParams.getBondTradeWebbondFmt());
        }else{
            jsonObjects = loadBondTradeWebbondFmt(date,configParams.getBondTradeWebbondFmtHistory());
        }

        if(CollectionUtils.isEmpty(jsonObjects)){
            logger.error("load bond_trade_webbond_fmt is null by = {}",date);
            return dbList;
        }

        StringBuilder sb = new StringBuilder();
        jsonObjects.forEach(jo -> {
            QuoteHistoryOriginals qho = new QuoteHistoryOriginals();
            String id = sb.append(
                    jo.getString("tradeId")).
                    append("_").
                    append(jo.getString("brokerId")).toString();
            try {
                qho.setQuoteTime(jo.getTimestamp("updateDateTime"));
                qho.setBondKey(jo.getString("bondKey"));
                qho.setListedMarket(jo.getString("listedMarket"));
                qho.setRemainTime(jo.getString("remaintTime"));
                qho.setRemainTimeValue(jo.getInteger("remaintTimeValue"));
                qho.setId(id);
                qho.setTransType(jo.getString("transType"));
                qho.setQuoteType(2);
                dbList.add(qho);
            } catch (Exception e) {
                logger.error("jsonObject convert db model error.   jsonObject={}.   exception={}",
                        JSON.toJSONString(jo),e.getMessage());
            }finally {
                sb.delete(0,sb.length());
            }
        });
        return dbList;
    }

    /**
     * 该方法需要过滤 trade 当中的 delete 和更新 update
     * @param list
     * @return
     */
    public List<QuoteHistories> getQHList(List<QuoteHistoryOriginals> list){
        HashMap<String,QuoteHistories> map = new HashMap<>();
        if(CollectionUtils.isEmpty(list)) return new ArrayList<>(map.values());
        logger.info("filter before size={}",list.size());
        list.sort(Comparator.comparing(QuoteHistoryOriginals::getQuoteTime));
        list.forEach(qho -> {
            String transType = qho.getTransType();
            if(StringUtils.isBlank(transType)) return;
            switch (transType){
                case "delete":map.remove(qho.getId());break;
                default:{
                    QuoteHistories qh = new QuoteHistories();
                    qh.setQuoteTime(qho.getQuoteTime());
                    qh.setQuoteType(qho.getQuoteType());
                    qh.setBondKey(qho.getBondKey());
                    qh.setListedMarket(qho.getListedMarket());
                    qh.setRemainTime(qho.getRemainTime());
                    qh.setRemainTimeValue(qho.getRemainTimeValue());
                    map.put(qho.getId(),qh);
                }
            }
        });
        logger.info("filter after size={}",map.size());
        return new ArrayList<>(map.values());
    }

}
