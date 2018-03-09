package com.sumscope.wf.bond.monitor.data.access.cdh;

import com.alibaba.fastjson.JSONObject;
import com.sumscope.wf.bond.monitor.global.ConfigParams;
import com.sumscope.wf.bond.monitor.global.constants.BondMonitorConstants;
import com.sumscope.wf.bond.monitor.utils.DateUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

@Component
public class CdhBeforeWorkdayLoader {
    private static final Logger logger = LoggerFactory.getLogger(CdhBeforeWorkdayLoader.class);
    @Autowired
    private ConfigParams configParams;
    @Autowired
    private CdhRestWrapper cdhRestWrapper;

    public List<JSONObject> loadCdhBeforeWorkday(){
        List<JSONObject> jsonObjectList = cdhRestWrapper.getJSONObjectList(
                configParams.getRestfulUrl(),
                configParams.getUsername(),
                configParams.getPassword(),
                BondMonitorConstants.RESTFUL_PAGE_SIZE,
                BondMonitorConstants.RESTFUL_DATASOURCE_ID_100,
                configParams.getQbproRecentNthWorkdaysCibName(),
                "",
                new ArrayList<>());
        logger.info("get qbpro_recent_Nth_workdays_CIB restful size={}", jsonObjectList.size());
        return jsonObjectList;
    }

    public String getPreWorkday(int day){
        List<JSONObject> list = loadCdhBeforeWorkday();
        String localDateString = DateUtils.getLocalDateString(BondMonitorConstants.DATE_YYYYMMDD);

        if(!CollectionUtils.isEmpty(list)){
            if(day > 0){
                if(!isWorkday(localDateString,list)) day--;
                return list.get(day).getString(BondMonitorConstants.REST_NTH_WORKDAY_API_NAME);
            }else
                logger.error("get pre workday param is not support. day={}",day);
        }else
            logger.error("get PreWorkDayCIB list size is 0.");
        return localDateString;
    }

    public List<String> getPreWorkdayList(int inc){
        logger.info("get getPreWorkdayList by {}", inc);
        ArrayList<String> workdays = new ArrayList<>();
        List<JSONObject> list = loadCdhBeforeWorkday();

        if(inc < 1 || CollectionUtils.isEmpty(list)) return workdays;
        do{
            workdays.add(list.get(inc - 1).getString(BondMonitorConstants.REST_NTH_WORKDAY_API_NAME));
            inc--;
        }while (inc > 0);
        logger.info("get getPreWorkdayList is= {}",workdays.toString());
        return workdays;
    }

    private boolean isWorkday(String day, List<JSONObject> list){
        return StringUtils.equalsIgnoreCase(
                list.get(0).getString(BondMonitorConstants.REST_NTH_WORKDAY_API_NAME),day);
    }
}
