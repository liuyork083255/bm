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
import java.util.List;

@Component
public class CdhBondMarketStreamLoader {
    private static final Logger logger = LoggerFactory.getLogger(CdhBondMarketStreamLoader.class);
    @Autowired
    private ConfigParams configParams;
    @Autowired
    private CdhRestWrapper cdhRestWrapper;
    @Autowired
    private QuoteHistorySaverUtil quoteHistorySaverUtil;

    private List<JSONObject> loadBondMarketStream(String date,String apiName){
        List<JSONObject> jsonObjectList = cdhRestWrapper.getJSONObjectList(
                configParams.getRestfulUrl(),
                configParams.getUsername(),
                configParams.getPassword(),
                BondMonitorConstants.RESTFUL_PAGE_SIZE,
                BondMonitorConstants.RESTFUL_DATASOURCE_ID_106,
                configParams.getBondMarketStreamWebFmt(),
                String.format("updateDateTime <= '%s 23:59:59' and updateDateTime >= '%s 00:00:00'",date,date),
                BondMonitorConstants.RESTFUL_BOND_MARKET_STREAM_WEBBOND_FMT);
        logger.info("load bond_market_stream_webbond_fmt size = {}",jsonObjectList.size());
        return jsonObjectList;
    }

    public List<QuoteHistoryOriginals> getQHOList(String date){
        logger.info("load bond_market_stream_webbond_fmt stating by = {}",date);

        List<QuoteHistoryOriginals> dbList = new ArrayList<>();

        List<JSONObject> jsonObjects;
        // 判断当前日期是否为本周，否则需要从历史表获取数据
        if(quoteHistorySaverUtil.isSameWeek(date)){
            jsonObjects = loadBondMarketStream(date, configParams.getBondMarketStreamWebFmt());
        }else{
            jsonObjects = loadBondMarketStream(date, configParams.getBondMarketStreamWebFmtHistory());
        }

        if(CollectionUtils.isEmpty(jsonObjects)){
            logger.error("load bond_market_stream_webbond_fmt is null by = {}",date);
            return dbList;
        }

        jsonObjects.forEach(jsonObject -> {
            QuoteHistoryOriginals qho = new QuoteHistoryOriginals();
            try {
                qho.setQuoteTime(jsonObject.getTimestamp("updateDateTime"));
                qho.setBondKey(jsonObject.getString("bondKey"));
                qho.setListedMarket(jsonObject.getString("listedMarket"));
                qho.setRemainTime(jsonObject.getString("remaintTime"));
                qho.setRemainTimeValue(jsonObject.getInteger("remaintTimeValue"));
                qho.setId(jsonObject.getString("uuid"));
                if(StringUtils.isNotBlank(jsonObject.getString("bidQuoteIds"))){
                    qho.setQuoteType(0);
                    dbList.add(qho);
                }
                if(StringUtils.isNotBlank(jsonObject.getString("ofrQuoteIds"))){
                    qho.setQuoteType(1);
                    dbList.add(qho);
                }
            } catch (Exception e) {
                logger.error("jsonObject convert db model error.   jsonObject={}.   exception={}",
                        JSON.toJSONString(jsonObject),e.getMessage());
            }
        });
        logger.info("convert size is {}",dbList.size());
        return dbList;
    }

    public List<QuoteHistories> getQHList(List<QuoteHistoryOriginals> dataList){
        List<QuoteHistories> list = new ArrayList<>();
        if(CollectionUtils.isEmpty(dataList)) return list;

        dataList.forEach(qho -> {
            QuoteHistories qh = new QuoteHistories();
            qh.setQuoteTime(qho.getQuoteTime());
            qh.setQuoteType(qho.getQuoteType());
            qh.setBondKey(qho.getBondKey());
            qh.setListedMarket(qho.getListedMarket());
            qh.setRemainTime(qho.getRemainTime());
            qh.setRemainTimeValue(qho.getRemainTimeValue());
            list.add(qh);
        });
        return list;
    }
}
