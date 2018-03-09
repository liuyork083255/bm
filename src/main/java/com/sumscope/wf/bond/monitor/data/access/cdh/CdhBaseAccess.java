package com.sumscope.wf.bond.monitor.data.access.cdh;

import com.sumscope.service.webbond.common.cdh.CdhConfig;
import com.sumscope.service.webbond.common.cdh.CdhRestClient;
import com.sumscope.service.webbond.common.cdh.request.CdhPagingArgs;
import com.sumscope.service.webbond.common.cdh.response.CdhPageResult;
import com.sumscope.service.webbond.common.http.exception.HttpException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class CdhBaseAccess {
    /**
     * 获取所有数据
     *
     * @return
     * @throws HttpException
     */
    protected <T> List<T> getAllRows(CdhConfig cdhConfig, int dataSourceId, String apiName, String apiVersion,
                                     Date startTime, Date endTime, List<String> codes, String condition,
                                     List<String> resultColumns, Class<T> clazz) throws HttpException {
        List<T> allRows = CdhRestClient.getAllRows(cdhConfig, dataSourceId, apiName, apiVersion,
                startTime, endTime, codes, condition, resultColumns, clazz);
        return CollectionUtils.isEmpty(allRows) ? new ArrayList<>() : allRows;
    }

    /**
     * 获取所有数据
     *
     * @return
     * @throws HttpException
     */
    protected <T> CdhPageResult<T> getPage(CdhConfig cdhConfig, int dataSourceId, String apiName,
                                           String apiVersion, Date startTime, Date endTime, List<String> codes,
                                           String condition, List<String> resultColumns, CdhPagingArgs pagingArgs,
                                           Class<T> clazz) throws HttpException {
        return CdhRestClient.getPageRows(cdhConfig, dataSourceId, apiName, apiVersion,
                startTime, endTime, codes, condition, resultColumns, pagingArgs, clazz);
    }
}
