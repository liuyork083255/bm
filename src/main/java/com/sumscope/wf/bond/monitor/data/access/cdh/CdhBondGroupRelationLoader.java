package com.sumscope.wf.bond.monitor.data.access.cdh;

import com.alibaba.fastjson.JSONObject;
import com.sumscope.wf.bond.monitor.global.ConfigParams;
import com.sumscope.wf.bond.monitor.global.constants.BondMonitorConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.List;

@Component
public class CdhBondGroupRelationLoader {
    private static final Logger logger = LoggerFactory.getLogger(CdhBondGroupRelationLoader.class);
    @Autowired
    private ConfigParams configParams;
    @Autowired
    private CdhRestWrapper cdhRestWrapper;

    public List<JSONObject> loadBondGroupRelation(String groupId){
        List<JSONObject> jsonObjectList = cdhRestWrapper.getJSONObjectList(
                configParams.getRestfulUrl(),
                configParams.getUsername(),
                configParams.getPassword(),
                BondMonitorConstants.RESTFUL_PAGE_SIZE,
                BondMonitorConstants.RESTFUL_DATASOURCE_ID_107,
                configParams.getBondGroupRelationApiName(),
                String.format("group_id = '%s'",groupId),
                BondMonitorConstants.RESTFUL_BOND_GROUP_RELATION_COLUMNS);
        logger.info("get bond_group_relation restful size={}", jsonObjectList.size());
        return jsonObjectList;
    }
}
