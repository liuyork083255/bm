package com.sumscope.wf.bond.monitor.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BondTypeUtil {
    private static final Logger logger = LoggerFactory.getLogger(BondTypeUtil.class);

    public static String getBondType(String bondKey,String bondType,String bondSubType){
        if(StringUtils.isBlank(bondType)){
            logger.error("{}'s bond_type is null",bondKey);
            return null;
        }
        switch (bondType){
            case "GOV":
            case "CBB":
            case "LGB":return "R";
            default:{
                if(StringUtils.equals("FIN",bondType) && StringUtils.equals("PBB",bondSubType)) return "R";
                else return "C";
            }
        }
    }
}
