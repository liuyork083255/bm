package com.sumscope.wf.bond.monitor.global.enums;

import com.sumscope.wf.bond.monitor.global.constants.BondMonitorConstants;

import java.util.ArrayList;
import java.util.List;

public enum  DiffValueFieldEnum {
    DIFF1(BondMonitorConstants.DIFF_FIVE, "price_diff1_count"),
    DIFF2(BondMonitorConstants.DIFF_TEN, "price_diff2_count"),
    DIFF3(BondMonitorConstants.DIFF_FIFTEEN, "price_diff3_count"),
    DIFF4(BondMonitorConstants.DIFF_TWENTY, "price_diff4_count"),
    DIFF5(BondMonitorConstants.DIFF_TWENTY_FIVE, "price_diff5_count"),;

    private DiffValueFieldEnum(String diffValue, String diffField){
        this.diffValue = diffValue;
        this.diffField = diffField;
    }

    private String diffValue;
    private String diffField;

    public String getDiffValue() {
        return diffValue;
    }

    public String getDiffField() {
        return diffField;
    }

    public static String getDiffField(String diffValue){
        DiffValueFieldEnum[] diffValueEmums = DiffValueFieldEnum.values();
        for (DiffValueFieldEnum diffValueEmum: diffValueEmums){
            if(diffValue.equals(diffValueEmum.getDiffValue())){
                return diffValueEmum.getDiffField();
            }
        }
        return null;
    }

    public static List<String> toList(){
        List<String> list = new ArrayList<>();
        for(DiffValueFieldEnum dfe : DiffValueFieldEnum.values()){
            list.add(dfe.getDiffValue());        }
        return list;
    }

}
