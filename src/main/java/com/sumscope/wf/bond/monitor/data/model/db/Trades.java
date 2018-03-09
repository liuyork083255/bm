package com.sumscope.wf.bond.monitor.data.model.db;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import java.math.BigDecimal;
import java.sql.Time;
import java.util.Objects;

@Entity
public class Trades {
    private long autoId;
    private String id;
    private String bondKey;
    private String bondCode;
    private String listedMarket;
    private String shortName;
    private String dealPrice;
    private BigDecimal dealPriceValue;
    private String cdcValuation;
    private BigDecimal cdcValuationValue;
    private String cdcDiff;
    private BigDecimal cdcDiffValue;
    private Time updateTime;
    private String remainTime;
    private String yieldCurve;


    @Column(name = "auto_id", nullable = false)
    public long getAutoId() {
        return autoId;
    }

    public void setAutoId(long autoId) {
        this.autoId = autoId;
    }

    @Id
    @Column(name = "id", nullable = false, length = 50)
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Basic
    @Column(name = "bond_key", nullable = true, length = 32)
    public String getBondKey() {
        return bondKey;
    }

    public void setBondKey(String bondKey) {
        this.bondKey = bondKey;
    }

    @Basic
    @Column(name = "bond_code", nullable = true, length = 18)
    public String getBondCode() {
        return bondCode;
    }

    public void setBondCode(String bondCode) {
        this.bondCode = bondCode;
    }

    @Basic
    @Column(name = "listed_market", nullable = true, length = 3)
    public String getListedMarket() {
        return listedMarket;
    }

    public void setListedMarket(String listedMarket) {
        this.listedMarket = listedMarket;
    }

    @Basic
    @Column(name = "short_name", nullable = true, length = 20)
    public String getShortName() {
        return shortName;
    }

    public void setShortName(String shortName) {
        this.shortName = shortName;
    }

    @Basic
    @Column(name = "deal_price", nullable = true, length = 10)
    public String getDealPrice() {
        return dealPrice;
    }

    public void setDealPrice(String dealPrice) {
        this.dealPrice = dealPrice;
    }

    @Basic
    @Column(name = "deal_price_value", nullable = true, precision = 4)
    public BigDecimal getDealPriceValue() {
        return dealPriceValue;
    }

    public void setDealPriceValue(BigDecimal dealPriceValue) {
        this.dealPriceValue = dealPriceValue;
    }

    @Basic
    @Column(name = "cdc_valuation", nullable = true, length = 10)
    public String getCdcValuation() {
        return cdcValuation;
    }

    public void setCdcValuation(String cdcValuation) {
        this.cdcValuation = cdcValuation;
    }

    @Basic
    @Column(name = "cdc_valuation_value", nullable = true, precision = 4)
    public BigDecimal getCdcValuationValue() {
        return cdcValuationValue;
    }

    public void setCdcValuationValue(BigDecimal cdcValuationValue) {
        this.cdcValuationValue = cdcValuationValue;
    }

    @Basic
    @Column(name = "cdc_diff", nullable = true, length = 10)
    public String getCdcDiff() {
        return cdcDiff;
    }

    public void setCdcDiff(String cdcDiff) {
        this.cdcDiff = cdcDiff;
    }

    @Basic
    @Column(name = "cdc_diff_value", nullable = true, precision = 4)
    public BigDecimal getCdcDiffValue() {
        return cdcDiffValue;
    }

    public void setCdcDiffValue(BigDecimal cdcDiffValue) {
        this.cdcDiffValue = cdcDiffValue;
    }

    @Basic
    @Column(name = "update_time", nullable = true)
    public Time getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Time updateTime) {
        this.updateTime = updateTime;
    }

    @Basic
    @Column(name = "remain_time", nullable = true, length = 32)
    public String getRemainTime() {
        return remainTime;
    }

    public void setRemainTime(String remainTime) {
        this.remainTime = remainTime;
    }

    @Basic
    @Column(name = "yield_curve", nullable = true, length = 30)
    public String getYieldCurve() {
        return yieldCurve;
    }

    public void setYieldCurve(String yieldCurve) {
        this.yieldCurve = yieldCurve;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Trades trades = (Trades) o;
        return autoId == trades.autoId &&
                Objects.equals(id, trades.id) &&
                Objects.equals(bondKey, trades.bondKey) &&
                Objects.equals(bondCode, trades.bondCode) &&
                Objects.equals(listedMarket, trades.listedMarket) &&
                Objects.equals(shortName, trades.shortName) &&
                Objects.equals(dealPrice, trades.dealPrice) &&
                Objects.equals(dealPriceValue, trades.dealPriceValue) &&
                Objects.equals(cdcValuation, trades.cdcValuation) &&
                Objects.equals(cdcValuationValue, trades.cdcValuationValue) &&
                Objects.equals(cdcDiff, trades.cdcDiff) &&
                Objects.equals(cdcDiffValue, trades.cdcDiffValue) &&
                Objects.equals(updateTime, trades.updateTime) &&
                Objects.equals(remainTime, trades.remainTime) &&
                Objects.equals(yieldCurve, trades.yieldCurve);
    }

    @Override
    public int hashCode() {

        return Objects.hash(autoId, id, bondKey, bondCode, listedMarket, shortName, dealPrice, dealPriceValue, cdcValuation, cdcValuationValue, cdcDiff, cdcDiffValue, updateTime, remainTime, yieldCurve);
    }
}
