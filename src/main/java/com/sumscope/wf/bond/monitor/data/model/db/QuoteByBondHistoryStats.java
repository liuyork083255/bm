package com.sumscope.wf.bond.monitor.data.model.db;

import javax.persistence.*;
import java.math.BigDecimal;
import java.util.Objects;

@Entity
@Table(name = "quote_by_bond_history_stats", schema = "wf_bond_monitor", catalog = "")
public class QuoteByBondHistoryStats {
    private long autoId;
    private String bondKey;
    private String listedMarket;
    private int quoteType;
    private BigDecimal total;

    @Id
    @Column(name = "auto_id", nullable = false)
    public long getAutoId() {
        return autoId;
    }

    public void setAutoId(long autoId) {
        this.autoId = autoId;
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
    @Column(name = "quote_type", nullable = false)
    public int getQuoteType() {
        return quoteType;
    }

    public void setQuoteType(int quoteType) {
        this.quoteType = quoteType;
    }

    @Basic
    @Column(name = "total", nullable = true, precision = 4)
    public BigDecimal getTotal() {
        return total;
    }

    public void setTotal(BigDecimal total) {
        this.total = total;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QuoteByBondHistoryStats that = (QuoteByBondHistoryStats) o;
        return autoId == that.autoId &&
                quoteType == that.quoteType &&
                Objects.equals(bondKey, that.bondKey) &&
                Objects.equals(listedMarket, that.listedMarket) &&
                Objects.equals(total, that.total);
    }

    @Override
    public int hashCode() {

        return Objects.hash(autoId, bondKey, listedMarket, quoteType, total);
    }
}
