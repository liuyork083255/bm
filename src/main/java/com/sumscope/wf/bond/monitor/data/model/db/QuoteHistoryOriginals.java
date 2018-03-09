package com.sumscope.wf.bond.monitor.data.model.db;

import javax.persistence.*;
import java.sql.Timestamp;
import java.util.Objects;

@Entity
@Table(name = "quote_history_originals", schema = "wf_bond_monitor", catalog = "")
public class QuoteHistoryOriginals {
    private long autoId;
    private Timestamp quoteTime;
    private Integer quoteType;
    private String bondKey;
    private String listedMarket;
    private String remainTime;
    private Integer remainTimeValue;
    private String id;
    private String transType;

    @Id
    @Column(name = "auto_id", nullable = false)
    public long getAutoId() {
        return autoId;
    }

    public void setAutoId(long autoId) {
        this.autoId = autoId;
    }

    @Basic
    @Column(name = "quote_time", nullable = true)
    public Timestamp getQuoteTime() {
        return quoteTime;
    }

    public void setQuoteTime(Timestamp quoteTime) {
        this.quoteTime = quoteTime;
    }

    @Basic
    @Column(name = "quote_type", nullable = true)
    public Integer getQuoteType() {
        return quoteType;
    }

    public void setQuoteType(Integer quoteType) {
        this.quoteType = quoteType;
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
    @Column(name = "remain_time", nullable = true, length = 25)
    public String getRemainTime() {
        return remainTime;
    }

    public void setRemainTime(String remainTime) {
        this.remainTime = remainTime;
    }

    @Basic
    @Column(name = "remain_time_value", nullable = true)
    public Integer getRemainTimeValue() {
        return remainTimeValue;
    }

    public void setRemainTimeValue(Integer remainTimeValue) {
        this.remainTimeValue = remainTimeValue;
    }

    @Basic
    @Column(name = "trade_id", nullable = true, length = 32)
    public String getId() {
        return id;
    }

    public void setId(String tradeId) {
        this.id = tradeId;
    }

    @Basic
    @Column(name = "trans_type", nullable = true, length = 15)
    public String getTransType() {
        return transType;
    }

    public void setTransType(String transType) {
        this.transType = transType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QuoteHistoryOriginals that = (QuoteHistoryOriginals) o;
        return autoId == that.autoId &&
                Objects.equals(quoteTime, that.quoteTime) &&
                Objects.equals(quoteType, that.quoteType) &&
                Objects.equals(bondKey, that.bondKey) &&
                Objects.equals(listedMarket, that.listedMarket) &&
                Objects.equals(remainTime, that.remainTime) &&
                Objects.equals(remainTimeValue, that.remainTimeValue) &&
                Objects.equals(id, that.id) &&
                Objects.equals(transType, that.transType);
    }

    @Override
    public int hashCode() {

        return Objects.hash(autoId, quoteTime, quoteType, bondKey, listedMarket, remainTime, remainTimeValue, id, transType);
    }
}
