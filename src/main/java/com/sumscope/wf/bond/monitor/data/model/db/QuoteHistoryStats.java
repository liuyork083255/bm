package com.sumscope.wf.bond.monitor.data.model.db;

import javax.persistence.*;
import java.math.BigDecimal;
import java.util.Objects;

@Entity
@Table(name = "quote_history_stats", schema = "wf_bond_monitor", catalog = "")
@IdClass(QuoteHistoryStatsPK.class)
public class QuoteHistoryStats {
    private long autoId;
    private String startTime;
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
    @Column(name = "start_time", nullable = false, length = 5)
    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
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
        QuoteHistoryStats that = (QuoteHistoryStats) o;
        return autoId == that.autoId &&
                quoteType == that.quoteType &&
                Objects.equals(startTime, that.startTime) &&
                Objects.equals(total, that.total);
    }

    @Override
    public int hashCode() {

        return Objects.hash(autoId, startTime, quoteType, total);
    }
}
