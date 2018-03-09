package com.sumscope.wf.bond.monitor.global.enums;

import org.apache.commons.lang3.StringUtils;

public enum SaveDBTypeEnum {
    JDBC("jdbc"),JPA("jpa");
    private String type;
    SaveDBTypeEnum(String type){
        this.type = type;
    }

    public boolean equal(SaveDBTypeEnum saveDBTypeEnum){
        return StringUtils.equalsIgnoreCase(this.type,saveDBTypeEnum.type);
    }
}
