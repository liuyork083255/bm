package com.sumscope.wf.bond.monitor.utils;

import com.sumscope.service.webbond.common.web.response.ErrorNum;
import com.sumscope.service.webbond.common.web.response.MetaData;
import com.sumscope.service.webbond.common.web.response.ResponseData;
import com.sumscope.service.webbond.common.web.response.VoidData;
import com.sumscope.wf.bond.monitor.domain.monitor.WarnDTO;
import org.springframework.http.HttpStatus;

import javax.servlet.http.HttpServletRequest;

public class ResponseUtil {
    private ResponseUtil() {
    }
    public static ResponseData<VoidData> error(HttpStatus httpStatus, String errMsg, HttpServletRequest request) {
        return new ResponseData(new MetaData(httpStatus.value(), errMsg, request.getMethod(), request.getRequestURI()), VoidData.VOID);
    }

    public static ResponseData warn(String msg){
        WarnDTO warnDTO = new WarnDTO(msg);
        return new ResponseData(new MetaData(ErrorNum.SUCCESS.value(), "warn", null, null), warnDTO);
    }

}
