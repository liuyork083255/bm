package com.sumscope.wf.bond.monitor.global.constants;

import java.util.Arrays;
import java.util.List;

public class BondMonitorConstants {
    private BondMonitorConstants() {}
    public static final String DATE_YYYY_MM_DD = "yyyy-MM-dd";
    public static final String DATE_YYYYMMDD = "yyyyMMdd";
    public static final String DATE_YMD_HMS = "yyyy-MM-dd HH:mm:ss";
    public static final String DATE_HMS = "HH:mm:ss";
    public static final String DATE_YMD_HMS_ZONE = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    public static final String BOND_KEY = "bond_key";
    public static final String LISTED_MARKET = "listed_market";
    public static final String YIELD_CURVE_ID = "Yield_Curve_ID";
    public static final String CREDIBILITY = "Credibility";
    public static final String YIELD_CURVE_PERIOD = "Period";
    public static final String YIELD_CURVE_YIELD = "Yield";
    public static final String REST_NTH_WORKDAY_API_NAME = "recent_Nth_workday";
    public static final int RESTFUL_PAGE_SIZE = 5000;
    public static final int RESTFUL_DATASOURCE_ID_100 = 100;
    public static final int RESTFUL_DATASOURCE_ID_106 = 106;
    public static final int RESTFUL_DATASOURCE_ID_107 = 107;
    public static final String DIFF_FIVE = "5";
    public static final String DIFF_TEN = "10";
    public static final String DIFF_FIFTEEN = "15";
    public static final String DIFF_TWENTY = "20";
    public static final String DIFF_TWENTY_FIVE = "25";
    public static final List<String> RESTFUL_BOND_GROUP_RELATION_COLUMNS = Arrays.asList("bond_key","market_id");
    public static final List<String> RESTFUL_CDC_BOND_VALUATION_COLUMNS
            = Arrays.asList(BOND_KEY,LISTED_MARKET,YIELD_CURVE_ID,CREDIBILITY);
    public static final List<String> RESTFUL_P_BOND_LIST_INFO_COLUMNS = Arrays.asList(BOND_KEY,LISTED_MARKET);
    public static final List<String> RESTFUL_BOND_MARKET_STREAM_WEBBOND_FMT = Arrays.asList(
            "uuid","bondKey","listedMarket","brokerId","remaintTime","remaintTimeValue",
            "updateDateTime","ofrQuoteIds","bidQuoteIds"
    );
    public static final List<String> RESTFUL_BOND_TRADE_WEBBOND_FMT = Arrays.asList(
            "tradeId","bondKey","listedMarket","brokerId","remaintTime","remaintTimeValue",
            "updateDateTime","transType"
    );
    public static final List<String> RESTFUL_BOND_COLUMNS = Arrays.asList(
            "bond_key","p_issuer_rating_current","issuer_code","short_name","bond_subtype",
            "is_municipal","bond_type","bond_subtype"
    );
    public static final List<String> RESTFUL_P_ISSUER_INFO_COLUMNS = Arrays.asList(
            "institution_code","sw_sector_code","sw_subsector_code","listed_type"
    );
    public static final List<String> RESTFUL_INSTITUTION_COLUMNS = Arrays.asList(
            "institution_code","full_name_c","cbrc_financing_platform","institution_subtype","province"
    );
}
