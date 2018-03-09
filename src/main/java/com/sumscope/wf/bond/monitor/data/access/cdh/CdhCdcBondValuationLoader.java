package com.sumscope.wf.bond.monitor.data.access.cdh;

import com.alibaba.fastjson.JSONObject;
import com.sumscope.wf.bond.monitor.data.model.db.BondInfos;
import com.sumscope.wf.bond.monitor.global.ConfigParams;
import com.sumscope.wf.bond.monitor.global.constants.BondMonitorConstants;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class CdhCdcBondValuationLoader {
    private static final Logger logger = LoggerFactory.getLogger(CdhCdcBondValuationLoader.class);
    @Autowired
    private ConfigParams configParams;
    @Autowired
    private CdhRestWrapper cdhRestWrapper;
    @Autowired
    private CdhBeforeWorkdayLoader cdhBeforeWorkdayLoader;

    public List<JSONObject> loadCdcBondValuation(String condition){
        List<JSONObject> jsonObjectList = cdhRestWrapper.getJSONObjectList(
                configParams.getRestfulUrl(),
                configParams.getUsername(),
                configParams.getPassword(),
                BondMonitorConstants.RESTFUL_PAGE_SIZE,
                BondMonitorConstants.RESTFUL_DATASOURCE_ID_100,
                configParams.getCdcBondValuationName(),
                String.format("Valuate_Date = '%s'",condition),
                BondMonitorConstants.RESTFUL_CDC_BOND_VALUATION_COLUMNS);
        logger.info("load cdc_bond_valuation size = {}",jsonObjectList.size());
        return jsonObjectList;
    }

    /**
     * 获取前一天的中债估值，然后将 Yield_Curve_ID 字段注入 {@link BondInfos#yieldCurveCode}
     * @param globalBondInfoMap
     */
    public void attributeInject(Map<String,Map<String,BondInfos>> globalBondInfoMap){
        logger.info("inject cdc_bond_valuation start ...");
        String preWorkday = cdhBeforeWorkdayLoader.getPreWorkday(1);
        List<JSONObject> jsonObjects = loadCdcBondValuation(preWorkday);
        if(CollectionUtils.isEmpty(jsonObjects)){
            logger.error("get cdc size is 0 by {}",preWorkday);
            return;
        }

        jsonObjects.forEach(jsonObject -> {
            String bondKey = jsonObject.getString(BondMonitorConstants.BOND_KEY);
            String listedMarket = jsonObject.getString(BondMonitorConstants.LISTED_MARKET);
            Map<String, BondInfos> stringBondInfosMap = globalBondInfoMap.get(bondKey);
            // 先根据 bondKey 获取 map，为空则表明 p_bond_list_info 中没有这只券
            if(!CollectionUtils.isEmpty(stringBondInfosMap)){
                // 然后根据 listedMarket 获取 map，为空则表明 p_bond_list_info 中没有该市场的债券
                BondInfos bondInfos = stringBondInfosMap.get(listedMarket);
                if(bondInfos != null) {
                    // 如果存在该市场的债券，则注入属性
                    // 在注入的时候需要判断当前估值 是否标记有 推荐 字段，如果有，则优先获取
                    String yieldCurveCode = bondInfos.getYieldCurveCode();
                    String yieldCurveID = jsonObject.getString(BondMonitorConstants.YIELD_CURVE_ID);
                    if(StringUtils.isBlank(yieldCurveCode)){
                        bondInfos.setYieldCurveCode(yieldCurveID);
                    }else{
                        String string = jsonObject.getString(BondMonitorConstants.CREDIBILITY);
                        if(StringUtils.equals("推荐",string)) bondInfos.setYieldCurveCode(yieldCurveID);
                    }
                }
            }
        });
        logger.info("inject cdc_bond_valuation end");
    }
}
