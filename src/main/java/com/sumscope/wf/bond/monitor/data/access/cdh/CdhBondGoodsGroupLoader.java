package com.sumscope.wf.bond.monitor.data.access.cdh;

import com.alibaba.fastjson.JSONObject;
import com.sumscope.wf.bond.monitor.global.ConfigParams;
import com.sumscope.wf.bond.monitor.global.constants.BondMonitorConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

@Component
public class CdhBondGoodsGroupLoader {
    private static final Logger logger = LoggerFactory.getLogger(CdhBondGoodsGroupLoader.class);
    @Autowired
    private ConfigParams configParams;
    @Autowired
    private CdhRestWrapper cdhRestWrapper;

    public List<JSONObject> loadBondGoodsGroup(String userId){
        List<JSONObject> jsonObjectList = cdhRestWrapper.getJSONObjectList(
                configParams.getRestfulUrl(),
                configParams.getUsername(),
                configParams.getPassword(),
                BondMonitorConstants.RESTFUL_PAGE_SIZE,
                BondMonitorConstants.RESTFUL_DATASOURCE_ID_107,
                configParams.getBondGoodsGroupApiName(),
                String.format("status=1 and product = 'Bond' and ua_id = '%s'",userId),
                new ArrayList<>());
        logger.info("get bond_goods_group restful size={}", jsonObjectList.size());
        return jsonObjectList;
    }
}
