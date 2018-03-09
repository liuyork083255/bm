package com.sumscope.wf.bond.monitor.data.access.cdh;

import com.alibaba.fastjson.JSONObject;
import com.sumscope.wf.bond.monitor.data.model.cache.YieldCurveModel;
import com.sumscope.wf.bond.monitor.global.ConfigParams;
import com.sumscope.wf.bond.monitor.global.constants.BondMonitorConstants;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.*;

@Component
public class CdhYieldCurveLoader {
    private static final Logger logger = LoggerFactory.getLogger(CdhBondLoader.class);
    @Autowired
    private ConfigParams configParams;
    @Autowired
    private CdhRestWrapper cdhRestWrapper;
    private Map<String,YieldCurveModel> yieldCurveCache = new HashMap<>();

    public List<JSONObject> loadYieldCurve(String condition){
        List<JSONObject> jsonObjectList = cdhRestWrapper.getJSONObjectList(
                configParams.getRestfulUrl(),
                configParams.getUsername(),
                configParams.getPassword(),
                BondMonitorConstants.RESTFUL_PAGE_SIZE,
                BondMonitorConstants.RESTFUL_DATASOURCE_ID_100,
                configParams.getYieldCurveApiName(),
                String.format("CurveType = '%s'",condition),
                new ArrayList<>());
        logger.info("load YieldCurve size = {}",jsonObjectList.size());
        return jsonObjectList;
    }

    public YieldCurveModel getYieldCurve(String curveType){
        if(StringUtils.isBlank(curveType)) return new YieldCurveModel();
        else if(yieldCurveCache.get(curveType) == null) return convertJoToModelByCode(curveType);
        else return yieldCurveCache.get(curveType);
    }

    /**
     * 将 restful 获取的 JSONObject 对象转换为 YieldCurveModel
     * 同时将转换后的 YieldCurveModel 存入缓存
     * @param curveType
     * @return
     */
    private YieldCurveModel convertJoToModelByCode(String curveType){
        YieldCurveModel yieldCurveModel = new YieldCurveModel();
        List<JSONObject> jsonObjects = loadYieldCurve(curveType);
        if(CollectionUtils.isEmpty(jsonObjects)) return yieldCurveModel;

        jsonObjects.sort(Comparator.comparing(jo -> {
            String period = jo.getString(BondMonitorConstants.YIELD_CURVE_PERIOD);
            if(StringUtils.contains(period,"M") && period.length() > 1){
                return Integer.parseInt(period.substring(0,period.length() - 1)) * 30;
            }else if(StringUtils.contains(period,"Y") && period.length() > 1){
                return Integer.parseInt(period.substring(0,period.length() - 1)) * 360;
            }else{
                return Integer.MAX_VALUE;
            }
        }));

        List<String> periodList = new ArrayList<>();
        List<String> yieldList = new ArrayList<>();
        jsonObjects.forEach(jo -> {
            periodList.add(jo.getString(BondMonitorConstants.YIELD_CURVE_PERIOD));
            yieldList.add(jo.getString(BondMonitorConstants.YIELD_CURVE_YIELD));
        });
        yieldCurveModel.setPeriod(periodList);
        yieldCurveModel.setYield(yieldList);
        this.yieldCurveCache.put(curveType,yieldCurveModel);
        return yieldCurveModel;
    }

    public void clearYieldCurveCache(){
        yieldCurveCache.clear();
    }
}
