package com.sumscope.wf.bond.monitor.data.access.cdh;

import com.alibaba.fastjson.JSONObject;
import com.sumscope.wf.bond.monitor.data.model.db.BondInfos;
import com.sumscope.wf.bond.monitor.global.ConfigParams;
import com.sumscope.wf.bond.monitor.global.constants.BondMonitorConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class CdhWebBondLoader {
    private static final Logger logger = LoggerFactory.getLogger(CdhWebBondLoader.class);
    @Autowired
    private ConfigParams configParams;
    @Autowired
    private CdhRestWrapper cdhRestWrapper;

    public List<JSONObject> loadPIssuerInfo(String condition){
        List<JSONObject> jsonObjectList = cdhRestWrapper.getJSONObjectList(
                configParams.getRestfulUrl(),
                configParams.getUsername(),
                configParams.getPassword(),
                BondMonitorConstants.RESTFUL_PAGE_SIZE,
                BondMonitorConstants.RESTFUL_DATASOURCE_ID_100,
                configParams.getWebBondApiName(),
                condition,
                new ArrayList<>());
        logger.info("load webbond_bond size = {}",jsonObjectList.size());
        return jsonObjectList;
    }

    /**
     * 这个时候参数 globalBondInfoList 分组是 issuerCode
     * 根据 condition 条件获取所有 institution，然后将属性注入 {@link BondInfos}
     * @param globalBondInfoMap
     * @param condition
     */
    public void attributeInject(Map<String,Map<String,BondInfos>> globalBondInfoMap, String condition){

    }
}
