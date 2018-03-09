package com.sumscope.wf.bond.monitor.data.model.db;

import javax.persistence.*;
import java.math.BigDecimal;
import java.sql.Time;
import java.util.Objects;

@Entity
@Table(name = "trade_stats", schema = "wf_bond_monitor", catalog = "")
public class TradeStats {
    private long autoId;
    private String bondCode;
    private String bondKey;
    private String listedMarket;
    private String shortName;
    private BigDecimal cdcPrice;
    private BigDecimal avgDiffPrice;
    private BigDecimal latestPrice;
    private BigDecimal latestPriceDiff;
    private Integer priceDiff1Count;
    private Integer priceDiff2Count;
    private Integer priceDiff3Count;
    private Integer priceDiff4Count;
    private Integer priceDiff5Count;
    private Integer priceDiff6Count;
    private Integer priceDiff7Count;
    private Integer priceDiff8Count;
    private Integer priceDiff9Count;
    private Integer priceDiff10Count;
    private Time updateTime;
    private String yieldCurve;
    private Integer tradeCount;


    @Column(name = "auto_id", nullable = false)
    public long getAutoId() {
        return autoId;
    }

    public void setAutoId(long autoId) {
        this.autoId = autoId;
    }

    @Id
    @Column(name = "bond_code", nullable = false, length = 25)
    public String getBondCode() {
        return bondCode;
    }

    public void setBondCode(String bondCode) {
        this.bondCode = bondCode;
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
    @Column(name = "listed_market", nullable = true, length = 3)
    public String getListedMarket() {
        return listedMarket;
    }

    public void setListedMarket(String listedMarket) {
        this.listedMarket = listedMarket;
    }

    @Basic
    @Column(name = "short_name", nullable = true, length = 32)
    public String getShortName() {
        return shortName;
    }

    public void setShortName(String shortName) {
        this.shortName = shortName;
    }

    @Basic
    @Column(name = "cdc_price", nullable = true, precision = 4)
    public BigDecimal getCdcPrice() {
        return cdcPrice;
    }

    public void setCdcPrice(BigDecimal cdcPrice) {
        this.cdcPrice = cdcPrice;
    }

    @Basic
    @Column(name = "avg_diff_price", nullable = true, precision = 4)
    public BigDecimal getAvgDiffPrice() {
        return avgDiffPrice;
    }

    public void setAvgDiffPrice(BigDecimal avgDiffPrice) {
        this.avgDiffPrice = avgDiffPrice;
    }

    @Basic
    @Column(name = "latest_price", nullable = true, precision = 4)
    public BigDecimal getLatestPrice() {
        return latestPrice;
    }

    public void setLatestPrice(BigDecimal latestPrice) {
        this.latestPrice = latestPrice;
    }

    @Basic
    @Column(name = "latest_price_diff", nullable = true, precision = 4)
    public BigDecimal getLatestPriceDiff() {
        return latestPriceDiff;
    }

    public void setLatestPriceDiff(BigDecimal latestPriceDiff) {
        this.latestPriceDiff = latestPriceDiff;
    }

    @Basic
    @Column(name = "price_diff1_count", nullable = true)
    public Integer getPriceDiff1Count() {
        return priceDiff1Count;
    }

    public void setPriceDiff1Count(Integer priceDiff1Count) {
        this.priceDiff1Count = priceDiff1Count;
    }

    @Basic
    @Column(name = "price_diff2_count", nullable = true)
    public Integer getPriceDiff2Count() {
        return priceDiff2Count;
    }

    public void setPriceDiff2Count(Integer priceDiff2Count) {
        this.priceDiff2Count = priceDiff2Count;
    }

    @Basic
    @Column(name = "price_diff3_count", nullable = true)
    public Integer getPriceDiff3Count() {
        return priceDiff3Count;
    }

    public void setPriceDiff3Count(Integer priceDiff3Count) {
        this.priceDiff3Count = priceDiff3Count;
    }

    @Basic
    @Column(name = "price_diff4_count", nullable = true)
    public Integer getPriceDiff4Count() {
        return priceDiff4Count;
    }

    public void setPriceDiff4Count(Integer priceDiff4Count) {
        this.priceDiff4Count = priceDiff4Count;
    }

    @Basic
    @Column(name = "price_diff5_count", nullable = true)
    public Integer getPriceDiff5Count() {
        return priceDiff5Count;
    }

    public void setPriceDiff5Count(Integer priceDiff5Count) {
        this.priceDiff5Count = priceDiff5Count;
    }

    @Basic
    @Column(name = "price_diff6_count", nullable = true)
    public Integer getPriceDiff6Count() {
        return priceDiff6Count;
    }

    public void setPriceDiff6Count(Integer priceDiff6Count) {
        this.priceDiff6Count = priceDiff6Count;
    }

    @Basic
    @Column(name = "price_diff7_count", nullable = true)
    public Integer getPriceDiff7Count() {
        return priceDiff7Count;
    }

    public void setPriceDiff7Count(Integer priceDiff7Count) {
        this.priceDiff7Count = priceDiff7Count;
    }

    @Basic
    @Column(name = "price_diff8_count", nullable = true)
    public Integer getPriceDiff8Count() {
        return priceDiff8Count;
    }

    public void setPriceDiff8Count(Integer priceDiff8Count) {
        this.priceDiff8Count = priceDiff8Count;
    }

    @Basic
    @Column(name = "price_diff9_count", nullable = true)
    public Integer getPriceDiff9Count() {
        return priceDiff9Count;
    }

    public void setPriceDiff9Count(Integer priceDiff9Count) {
        this.priceDiff9Count = priceDiff9Count;
    }

    @Basic
    @Column(name = "price_diff10_count", nullable = true)
    public Integer getPriceDiff10Count() {
        return priceDiff10Count;
    }

    public void setPriceDiff10Count(Integer priceDiff10Count) {
        this.priceDiff10Count = priceDiff10Count;
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
    @Column(name = "yield_curve", nullable = true, length = 20)
    public String getYieldCurve() {
        return yieldCurve;
    }

    public void setYieldCurve(String yieldCurve) {
        this.yieldCurve = yieldCurve;
    }

    @Basic
    @Column(name = "trade_count", nullable = true)
    public Integer getTradeCount() {
        return tradeCount;
    }

    public void setTradeCount(Integer tradeCount) {
        this.tradeCount = tradeCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TradeStats that = (TradeStats) o;
        return autoId == that.autoId &&
                Objects.equals(bondCode, that.bondCode) &&
                Objects.equals(bondKey, that.bondKey) &&
                Objects.equals(listedMarket, that.listedMarket) &&
                Objects.equals(shortName, that.shortName) &&
                Objects.equals(cdcPrice, that.cdcPrice) &&
                Objects.equals(avgDiffPrice, that.avgDiffPrice) &&
                Objects.equals(latestPrice, that.latestPrice) &&
                Objects.equals(latestPriceDiff, that.latestPriceDiff) &&
                Objects.equals(priceDiff1Count, that.priceDiff1Count) &&
                Objects.equals(priceDiff2Count, that.priceDiff2Count) &&
                Objects.equals(priceDiff3Count, that.priceDiff3Count) &&
                Objects.equals(priceDiff4Count, that.priceDiff4Count) &&
                Objects.equals(priceDiff5Count, that.priceDiff5Count) &&
                Objects.equals(priceDiff6Count, that.priceDiff6Count) &&
                Objects.equals(priceDiff7Count, that.priceDiff7Count) &&
                Objects.equals(priceDiff8Count, that.priceDiff8Count) &&
                Objects.equals(priceDiff9Count, that.priceDiff9Count) &&
                Objects.equals(priceDiff10Count, that.priceDiff10Count) &&
                Objects.equals(updateTime, that.updateTime) &&
                Objects.equals(yieldCurve, that.yieldCurve) &&
                Objects.equals(tradeCount, that.tradeCount);
    }

    @Override
    public int hashCode() {

        return Objects.hash(autoId, bondCode, bondKey, listedMarket, shortName, cdcPrice, avgDiffPrice, latestPrice, latestPriceDiff, priceDiff1Count, priceDiff2Count, priceDiff3Count, priceDiff4Count, priceDiff5Count, priceDiff6Count, priceDiff7Count, priceDiff8Count, priceDiff9Count, priceDiff10Count, updateTime, yieldCurve, tradeCount);
    }
}
