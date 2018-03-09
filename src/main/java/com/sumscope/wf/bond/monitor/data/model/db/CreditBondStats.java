package com.sumscope.wf.bond.monitor.data.model.db;

import javax.persistence.*;
import java.math.BigDecimal;
import java.util.Objects;

@Entity
@Table(name = "credit_bond_stats", schema = "wf_bond_monitor", catalog = "")
public class CreditBondStats {
    private long autoId;
    private String issuer;
    private Integer bidFif;
    private Integer ofrFif;
    private Integer tradeFif;
    private Integer bidAcc;
    private Integer ofrAcc;
    private Integer tradeAcc;
    private BigDecimal bidWeekAvg;
    private BigDecimal ofrWeekAvg;
    private BigDecimal tradeWeekAvg;
    private Integer bondCount;
    private Integer activeCount;
    private String activeBond;


    @Column(name = "auto_id", nullable = false)
    public long getAutoId() {
        return autoId;
    }

    public void setAutoId(long autoId) {
        this.autoId = autoId;
    }

    @Id
    @Column(name = "issuer", nullable = false, length = 32)
    public String getIssuer() {
        return issuer;
    }

    public void setIssuer(String issuer) {
        this.issuer = issuer;
    }

    @Basic
    @Column(name = "bid_fif", nullable = true)
    public Integer getBidFif() {
        return bidFif;
    }

    public void setBidFif(Integer bidFif) {
        this.bidFif = bidFif;
    }

    @Basic
    @Column(name = "ofr_fif", nullable = true)
    public Integer getOfrFif() {
        return ofrFif;
    }

    public void setOfrFif(Integer ofrFif) {
        this.ofrFif = ofrFif;
    }

    @Basic
    @Column(name = "trade_fif", nullable = true)
    public Integer getTradeFif() {
        return tradeFif;
    }

    public void setTradeFif(Integer tradeFif) {
        this.tradeFif = tradeFif;
    }

    @Basic
    @Column(name = "bid_acc", nullable = true)
    public Integer getBidAcc() {
        return bidAcc;
    }

    public void setBidAcc(Integer bidAcc) {
        this.bidAcc = bidAcc;
    }

    @Basic
    @Column(name = "ofr_acc", nullable = true)
    public Integer getOfrAcc() {
        return ofrAcc;
    }

    public void setOfrAcc(Integer ofrAcc) {
        this.ofrAcc = ofrAcc;
    }

    @Basic
    @Column(name = "trade_acc", nullable = true)
    public Integer getTradeAcc() {
        return tradeAcc;
    }

    public void setTradeAcc(Integer tradeAcc) {
        this.tradeAcc = tradeAcc;
    }

    @Basic
    @Column(name = "bid_week_avg", nullable = true, precision = 4)
    public BigDecimal getBidWeekAvg() {
        return bidWeekAvg;
    }

    public void setBidWeekAvg(BigDecimal bidWeekAvg) {
        this.bidWeekAvg = bidWeekAvg;
    }

    @Basic
    @Column(name = "ofr_week_avg", nullable = true, precision = 4)
    public BigDecimal getOfrWeekAvg() {
        return ofrWeekAvg;
    }

    public void setOfrWeekAvg(BigDecimal ofrWeekAvg) {
        this.ofrWeekAvg = ofrWeekAvg;
    }

    @Basic
    @Column(name = "trade_week_avg", nullable = true, precision = 4)
    public BigDecimal getTradeWeekAvg() {
        return tradeWeekAvg;
    }

    public void setTradeWeekAvg(BigDecimal tradeWeekAvg) {
        this.tradeWeekAvg = tradeWeekAvg;
    }

    @Basic
    @Column(name = "bond_count", nullable = true)
    public Integer getBondCount() {
        return bondCount;
    }

    public void setBondCount(Integer bondCount) {
        this.bondCount = bondCount;
    }

    @Basic
    @Column(name = "active_count", nullable = true)
    public Integer getActiveCount() {
        return activeCount;
    }

    public void setActiveCount(Integer activeCount) {
        this.activeCount = activeCount;
    }

    @Basic
    @Column(name = "active_bond", nullable = true, length = 15)
    public String getActiveBond() {
        return activeBond;
    }

    public void setActiveBond(String activeBond) {
        this.activeBond = activeBond;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreditBondStats that = (CreditBondStats) o;
        return autoId == that.autoId &&
                Objects.equals(issuer, that.issuer) &&
                Objects.equals(bidFif, that.bidFif) &&
                Objects.equals(ofrFif, that.ofrFif) &&
                Objects.equals(tradeFif, that.tradeFif) &&
                Objects.equals(bidAcc, that.bidAcc) &&
                Objects.equals(ofrAcc, that.ofrAcc) &&
                Objects.equals(tradeAcc, that.tradeAcc) &&
                Objects.equals(bidWeekAvg, that.bidWeekAvg) &&
                Objects.equals(ofrWeekAvg, that.ofrWeekAvg) &&
                Objects.equals(tradeWeekAvg, that.tradeWeekAvg) &&
                Objects.equals(bondCount, that.bondCount) &&
                Objects.equals(activeCount, that.activeCount) &&
                Objects.equals(activeBond, that.activeBond);
    }

    @Override
    public int hashCode() {

        return Objects.hash(autoId, issuer, bidFif, ofrFif, tradeFif, bidAcc, ofrAcc, tradeAcc, bidWeekAvg, ofrWeekAvg, tradeWeekAvg, bondCount, activeCount, activeBond);
    }
}
