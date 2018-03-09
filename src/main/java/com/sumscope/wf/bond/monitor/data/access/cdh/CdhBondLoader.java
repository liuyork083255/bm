package com.sumscope.wf.bond.monitor.data.access.cdh;

import com.alibaba.fastjson.JSONObject;
import com.sumscope.wf.bond.monitor.data.model.db.BondInfos;
import com.sumscope.wf.bond.monitor.global.ConfigParams;
import com.sumscope.wf.bond.monitor.global.constants.BondMonitorConstants;
import com.sumscope.wf.bond.monitor.utils.BondTypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class CdhBondLoader extends CdhBaseAccess{
    private static final Logger logger = LoggerFactory.getLogger(CdhBondLoader.class);
    @Autowired
    private ConfigParams configParams;
    @Autowired
    private CdhRestWrapper cdhRestWrapper;

    public List<JSONObject> loadBond(String condition){
        List<JSONObject> jsonObjectList = cdhRestWrapper.getJSONObjectList(
                configParams.getRestfulUrl(),
                configParams.getUsername(),
                configParams.getPassword(),
                BondMonitorConstants.RESTFUL_PAGE_SIZE,
                BondMonitorConstants.RESTFUL_DATASOURCE_ID_100,
                configParams.getBondApiName(),
                condition,
                BondMonitorConstants.RESTFUL_BOND_COLUMNS);
        logger.info("load bond size = {}",jsonObjectList.size());
        return jsonObjectList;
    }

    /**
     * 根据 condition 条件获取所有 bond，然后将属性注入 {@link BondInfos}
     * @param globalBondInfoMap
     * @param condition
     */
    public void attributeInject(Map<String,Map<String,BondInfos>> globalBondInfoMap, String condition){
        logger.info("inject bond start ...");
        List<JSONObject> jsonObjects = loadBond(condition);
        if(CollectionUtils.isEmpty(jsonObjects)) return;

        jsonObjects.forEach(jsonObject -> {
            String bondKey = jsonObject.getString(BondMonitorConstants.BOND_KEY);
            String bondType = jsonObject.getString("bond_type");
            String bondSubType = jsonObject.getString("bond_subtype");
            Map<String, BondInfos> stringBondInfosMap = globalBondInfoMap.get(bondKey);
            if(!CollectionUtils.isEmpty(stringBondInfosMap)){
                stringBondInfosMap.forEach((lm,bondInfo) ->{
                    bondInfo.setRating(jsonObject.getString("p_issuer_rating_current"));
                    bondInfo.setIssuerCode(jsonObject.getString("issuer_code"));
                    bondInfo.setShortName(jsonObject.getString("short_name"));
                    bondInfo.setBondSubType(jsonObject.getString("bond_subtype"));
                    bondInfo.setIsMunicipal(jsonObject.getString("is_municipal"));
                    // 根据 bond 表的 bondType bondSubType 两个字段判断是信用债还是利率债
                    bondInfo.setBondType(BondTypeUtil.getBondType(bondKey,bondType,bondSubType));
                });
            }
        });
        logger.info("inject bond end");
    }
}
