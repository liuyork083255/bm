package com.sumscope.wf.bond.monitor.utils;

import com.sumscope.wf.bond.monitor.data.model.db.TradeStats;
import com.sumscope.wf.bond.monitor.global.constants.BondMonitorConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BusinessUtils {
    private static final Logger logger = LoggerFactory.getLogger(BusinessUtils.class);

    private BusinessUtils(){}

    public static int getDiffValue(String diff, TradeStats tradeStats) {
        int diffValue = 0;
        switch (diff) {
            case BondMonitorConstants.DIFF_FIVE:
                diffValue = tradeStats.getPriceDiff1Count();
                break;
            case BondMonitorConstants.DIFF_TEN:
                diffValue = tradeStats.getPriceDiff2Count();
                break;
            case BondMonitorConstants.DIFF_FIFTEEN:
                diffValue = tradeStats.getPriceDiff3Count();
                break;
            case BondMonitorConstants.DIFF_TWENTY:
                diffValue = tradeStats.getPriceDiff4Count();
                break;
            case BondMonitorConstants.DIFF_TWENTY_FIVE:
                diffValue = tradeStats.getPriceDiff5Count();
                break;
            default:
                logger.error("the diff {} not in support list", diff);
                break;
        }
        return diffValue;
    }
}
