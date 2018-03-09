package com.sumscope.wf.bond.monitor.data.model.db;

import javax.persistence.*;
import java.sql.Time;
import java.util.Objects;

@Entity
@IdClass(QuotesPK.class)
public class Quotes {
    private long autoId;
    private String id;
    private int quoteType;
    private String bondKey;
    private String listedMarket;
    private Time quoteTime;
    private String rate;
    private Integer remainTimeValue;
    private String remainTime;
    private String issuerCode;
    private String term;

    @Id
    @Column(name = "auto_id", nullable = false)
    public long getAutoId() {
        return autoId;
    }

    public void setAutoId(long autoId) {
        this.autoId = autoId;
    }

    @Basic
    @Column(name = "id", nullable = false, length = 32)
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Basic
    @Column(name = "quote_type", nullable = false)
    public int getQuoteType() {
        return quoteType;
    }

    public void setQuoteType(int quoteType) {
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
    @Column(name = "quote_time", nullable = true)
    public Time getQuoteTime() {
        return quoteTime;
    }

    public void setQuoteTime(Time quoteTime) {
        this.quoteTime = quoteTime;
    }

    @Basic
    @Column(name = "rate", nullable = true, length = 10)
    public String getRate() {
        return rate;
    }

    public void setRate(String rate) {
        this.rate = rate;
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
    @Column(name = "remain_time", nullable = true, length = 15)
    public String getRemainTime() {
        return remainTime;
    }

    public void setRemainTime(String remainTime) {
        this.remainTime = remainTime;
    }

    @Basic
    @Column(name = "issuer_code", nullable = true, length = 32)
    public String getIssuerCode() {
        return issuerCode;
    }

    public void setIssuerCode(String issuerCode) {
        this.issuerCode = issuerCode;
    }

    @Basic
    @Column(name = "term", nullable = true, length = 15)
    public String getTerm() {
        return term;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Quotes quotes = (Quotes) o;
        return autoId == quotes.autoId &&
                quoteType == quotes.quoteType &&
                Objects.equals(id, quotes.id) &&
                Objects.equals(bondKey, quotes.bondKey) &&
                Objects.equals(listedMarket, quotes.listedMarket) &&
                Objects.equals(quoteTime, quotes.quoteTime) &&
                Objects.equals(rate, quotes.rate) &&
                Objects.equals(remainTimeValue, quotes.remainTimeValue) &&
                Objects.equals(remainTime, quotes.remainTime) &&
                Objects.equals(issuerCode, quotes.issuerCode) &&
                Objects.equals(term, quotes.term);
    }

    @Override
    public int hashCode() {

        return Objects.hash(autoId, id, quoteType, bondKey, listedMarket, quoteTime, rate, remainTimeValue, remainTime, issuerCode, term);
    }
}
