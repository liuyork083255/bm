package com.sumscope.wf.bond.monitor.data.access.cdh;

import com.alibaba.fastjson.JSONObject;
import com.sumscope.service.webbond.common.cdh.CdhConfig;
import com.sumscope.service.webbond.common.http.exception.HttpException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class CdhRestWrapper extends CdhBaseAccess {

    private static final Logger logger = LoggerFactory.getLogger(CdhRestWrapper.class);

    protected <T> List<T> getAllRowsSimple(String url,String username,String password,int pageSize,
                                           int dataSourceId,String apiName,String condition,
                                           List<String> resultColumns,Class<T> clazz)
            throws HttpException {
        return super.getAllRows(new CdhConfig(url,username,password,pageSize), dataSourceId,
                apiName, "N", null, null, null, condition,
                resultColumns, clazz);
    }

    public List<JSONObject> getJSONObjectList(String url,String username,String password,int pageSize,
                                              int dataSourceId,String apiName,String condition,
                                              List<String> resultColumns){
        List<JSONObject> allRowsSimple = new ArrayList<>();
        try {
            allRowsSimple = getAllRowsSimple(url, username, password, pageSize, dataSourceId, apiName,
                    condition, resultColumns, JSONObject.class);
        } catch (HttpException e) {
            logger.error("get restful={} error.    exception={}",apiName,e.getMessage());
        }
        return allRowsSimple;
    }
}
