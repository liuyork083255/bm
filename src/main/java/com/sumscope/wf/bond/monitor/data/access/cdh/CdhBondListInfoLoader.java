package com.sumscope.wf.bond.monitor.data.access.cdh;

import com.alibaba.fastjson.JSONObject;
import com.sumscope.wf.bond.monitor.global.ConfigParams;
import com.sumscope.wf.bond.monitor.global.constants.BondMonitorConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class CdhBondListInfoLoader {
    private static final Logger logger = LoggerFactory.getLogger(CdhBondListInfoLoader.class);
    @Autowired
    private ConfigParams configParams;
    @Autowired
    private CdhRestWrapper cdhRestWrapper;

    public List<JSONObject> loadCdhBondListInfo(String condition){
        List<JSONObject> jsonObjectList = cdhRestWrapper.getJSONObjectList(
                configParams.getRestfulUrl(),
                configParams.getUsername(),
                configParams.getPassword(),
                BondMonitorConstants.RESTFUL_PAGE_SIZE,
                BondMonitorConstants.RESTFUL_DATASOURCE_ID_100,
                configParams.getpBondListInfoName(),
                condition,
                BondMonitorConstants.RESTFUL_P_BOND_LIST_INFO_COLUMNS);
        logger.info("load p_bond_list_info size = {}",jsonObjectList.size());
        return jsonObjectList;
    }

    /**
     * 将获取bond info 转换为 key-value map
     * @param list
     * @return
     */
    public Map<String, JSONObject> convertMap(List<JSONObject> list){
        return list.stream().collect(Collectors.toMap(
                jo -> jo.getString(BondMonitorConstants.BOND_KEY) + "_" + jo.getString(BondMonitorConstants.LISTED_MARKET),
                jo -> jo));
    }
}
