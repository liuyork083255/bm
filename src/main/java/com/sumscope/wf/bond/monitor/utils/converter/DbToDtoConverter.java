package com.sumscope.wf.bond.monitor.utils.converter;

import com.sumscope.wf.bond.monitor.data.model.db.YieldCurves;
import com.sumscope.wf.bond.monitor.domain.monitor.CurveInfoDTO;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class DbToDtoConverter {

    public CurveInfoDTO convertYieldCurvesToCurveInfoDTO(List<YieldCurves> list){
        CurveInfoDTO curveInfoDTO = new CurveInfoDTO();
        if(CollectionUtils.isEmpty(list)) return curveInfoDTO;
        Map<String,String> map = new HashMap<>();
        list.forEach(yc -> map.put(yc.getCode(),yc.getName()));
        curveInfoDTO.setCurveInfoList(map);
        return curveInfoDTO;
    }
}
