package com.sumscope.wf.bond.monitor.global;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalTime;

@Component
public class ConfigParams {
    @Value("${cdh.restful.url}")
    private String restfulUrl;
    @Value("${cdh.restful.username}")
    private String username;
    @Value("${cdh.restful.password}")
    private String password;
    @Value("${cdh.restful.bond.apiName}")
    private String bondApiName;
    @Value("${spring.datasource.driver-class-name}")
    private String driver;
    @Value("${spring.datasource.url}")
    private String jdbcUrl;
    @Value("${spring.datasource.username}")
    private String jdbcUsername;
    @Value("${spring.datasource.password}")
    private String jdbcPassword;
    @Value("${spring.insert.bench.size}")
    private Integer benchSize;
    @Value("${application.first.load.bond.infos.is}")
    private Boolean isLoadBondInfo;
    @Value("${application.first.load.quote.history.days}")
    private Integer loadQuoteHistoryDays;
    @Value("${cdh.restful.yieldCurve.apiName}")
    private String yieldCurveApiName;
    @Value("${cdh.restful.institution.apiName}")
    private String institutionApiName;
    @Value("${cdh.restful.issuer.info.apiName}")
    private String pIssuerInfoApiName;
    @Value("${cdh.restful.webbond.bond.apiName}")
    private String webBondApiName;
    @Value("${cdh.restful.pBondListInfo.apiName}")
    private String pBondListInfoName;
    @Value("${cdh.restful.bond_trade_webbond_fmt.apiName}")
    private String bondTradeWebbondFmt;
    @Value("${cdh.restful.cdcBondValuation.apiName}")
    private String cdcBondValuationName;
    @Value("${cdh.restful.bondGoodsGroup.apiName}")
    private String bondGoodsGroupApiName;
    @Value("${cdh.restful.bond_market_stream_webbond_fmt.apiName}")
    private String bondMarketStreamWebFmt;
    @Value("${cdh.restful.bondGroupRelation.apiName}")
    private String bondGroupRelationApiName;
    @Value("${cdh.restful.qbproRecentNthWorkdaysCIB.apiName}")
    private String qbproRecentNthWorkdaysCibName;
    @Value("${cdh.restful.bond_trade_webbond_fmt.history.apiName}")
    private String bondTradeWebbondFmtHistory;
    @Value("${cdh.restful.bond_market_stream_webbond_fmt.history.apiName}")
    private String bondMarketStreamWebFmtHistory;
    @Value("${application.bond.active.range.time.start}")
    private String rangeStartTime;
    @Value("${application.bond.active.range.time.end}")
    private String rangeEndTime;
    @Value("${application.bond.active.time.interval}")
    private Integer intervalMin;

    public String getRestfulUrl() {
        return restfulUrl;
    }

    public void setRestfulUrl(String restfulUrl) {
        this.restfulUrl = restfulUrl;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getBondApiName() {
        return bondApiName;
    }

    public void setBondApiName(String bondApiName) {
        this.bondApiName = bondApiName;
    }

    public String getYieldCurveApiName() {
        return yieldCurveApiName;
    }

    public void setYieldCurveApiName(String yieldCurveApiName) {
        this.yieldCurveApiName = yieldCurveApiName;
    }

    public String getBondGoodsGroupApiName() {
        return bondGoodsGroupApiName;
    }

    public void setBondGoodsGroupApiName(String bondGoodsGroupApiName) {
        this.bondGoodsGroupApiName = bondGoodsGroupApiName;
    }

    public String getBondGroupRelationApiName() {
        return bondGroupRelationApiName;
    }

    public void setBondGroupRelationApiName(String bondGroupRelationApiName) {
        this.bondGroupRelationApiName = bondGroupRelationApiName;
    }

    public String getpBondListInfoName() {
        return pBondListInfoName;
    }

    public void setpBondListInfoName(String pBondListInfoName) {
        this.pBondListInfoName = pBondListInfoName;
    }

    public String getCdcBondValuationName() {
        return cdcBondValuationName;
    }

    public void setCdcBondValuationName(String cdcBondValuationName) {
        this.cdcBondValuationName = cdcBondValuationName;
    }

    public String getQbproRecentNthWorkdaysCibName() {
        return qbproRecentNthWorkdaysCibName;
    }

    public void setQbproRecentNthWorkdaysCibName(String qbproRecentNthWorkdaysCibName) {
        this.qbproRecentNthWorkdaysCibName = qbproRecentNthWorkdaysCibName;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getJdbcUsername() {
        return jdbcUsername;
    }

    public void setJdbcUsername(String jdbcUsername) {
        this.jdbcUsername = jdbcUsername;
    }

    public String getJdbcPassword() {
        return jdbcPassword;
    }

    public void setJdbcPassword(String jdbcPassword) {
        this.jdbcPassword = jdbcPassword;
    }

    public Integer getBenchSize() {
        return benchSize;
    }

    public void setBenchSize(Integer benchSize) {
        this.benchSize = benchSize;
    }

    public Boolean getLoadBondInfo() {
        return isLoadBondInfo;
    }

    public void setLoadBondInfo(Boolean loadBondInfo) {
        isLoadBondInfo = loadBondInfo;
    }

    public String getBondMarketStreamWebFmt() {
        return bondMarketStreamWebFmt;
    }

    public void setBondMarketStreamWebFmt(String bondMarketStreamWebFmt) {
        this.bondMarketStreamWebFmt = bondMarketStreamWebFmt;
    }

    public String getBondTradeWebbondFmt() {
        return bondTradeWebbondFmt;
    }

    public void setBondTradeWebbondFmt(String bondTradeWebbondFmt) {
        this.bondTradeWebbondFmt = bondTradeWebbondFmt;
    }

    public Integer getLoadQuoteHistoryDays() {
        return loadQuoteHistoryDays;
    }

    public void setLoadQuoteHistoryDays(Integer loadQuoteHistoryDays) {
        this.loadQuoteHistoryDays = loadQuoteHistoryDays;
    }

    public String getBondTradeWebbondFmtHistory() {
        return bondTradeWebbondFmtHistory;
    }

    public void setBondTradeWebbondFmtHistory(String bondTradeWebbondFmtHistory) {
        this.bondTradeWebbondFmtHistory = bondTradeWebbondFmtHistory;
    }

    public String getBondMarketStreamWebFmtHistory() {
        return bondMarketStreamWebFmtHistory;
    }

    public void setBondMarketStreamWebFmtHistory(String bondMarketStreamWebFmtHistory) {
        this.bondMarketStreamWebFmtHistory = bondMarketStreamWebFmtHistory;
    }

    public LocalTime getRangeStartTime() {
        return LocalTime.parse(rangeStartTime);
    }

    public void setRangeStartTime(String rangeStartTime) {
        this.rangeStartTime = rangeStartTime;
    }

    public LocalTime getRangeEndTime() {
        return LocalTime.parse(rangeEndTime);
    }

    public void setRangeEndTime(String rangeEndTime) {
        this.rangeEndTime = rangeEndTime;
    }

    public Integer getIntervalMin() {
        return intervalMin;
    }

    public void setIntervalMin(Integer intervalMin) {
        this.intervalMin = intervalMin;
    }

    public String getInstitutionApiName() {
        return institutionApiName;
    }

    public void setInstitutionApiName(String institutionApiName) {
        this.institutionApiName = institutionApiName;
    }

    public String getpIssuerInfoApiName() {
        return pIssuerInfoApiName;
    }

    public void setpIssuerInfoApiName(String pIssuerInfoApiName) {
        this.pIssuerInfoApiName = pIssuerInfoApiName;
    }

    public String getWebBondApiName() {
        return webBondApiName;
    }

    public void setWebBondApiName(String webBondApiName) {
        this.webBondApiName = webBondApiName;
    }
}
