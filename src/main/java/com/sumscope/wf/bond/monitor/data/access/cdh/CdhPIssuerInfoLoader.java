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

import java.util.List;
import java.util.Map;

@Component
public class CdhPIssuerInfoLoader {
    private static final Logger logger = LoggerFactory.getLogger(CdhPIssuerInfoLoader.class);
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
                configParams.getpIssuerInfoApiName(),
                condition,
                BondMonitorConstants.RESTFUL_P_ISSUER_INFO_COLUMNS);
        logger.info("load p_issuer_info size = {}",jsonObjectList.size());
        return jsonObjectList;
    }

    /**
     * 这个时候参数 globalBondInfoList 分组是 issuerCode
     * 根据 condition 条件获取所有 p_issuer_info，然后将属性注入 {@link BondInfos}
     * @param globalBondInfoList
     * @param condition
     */
    public void attributeInject(Map<String,List<BondInfos>> globalBondInfoList, String condition){
        logger.info("inject p_issuer_info start ...");
        List<JSONObject> jsonObjects = loadPIssuerInfo(condition);
        if(CollectionUtils.isEmpty(jsonObjects)) return;

        // institution_code 和 issuer_code 是同一个概念，这里属性注入是根据这条件一一对应的
        jsonObjects.forEach(jsonObject -> {
            String issuerCode = jsonObject.getString("institution_code");

            // 获取指定机构的所有债券
            List<BondInfos> bondInfoList = globalBondInfoList.get(issuerCode);
            if(!CollectionUtils.isEmpty(bondInfoList)){
                bondInfoList.forEach(bi -> {
                    bi.setCategory(jsonObject.getString("sw_sector_code"));
                    bi.setSubCategory(jsonObject.getString("sw_subsector_code"));
                    bi.setListedType(jsonObject.getString("listed_type"));
                });
            }
        });
        logger.info("inject p_issuer_info end");
    }
}
