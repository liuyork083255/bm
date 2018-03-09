package com.sumscope.wf.bond.monitor.data.model.db;

import javax.persistence.Column;
import javax.persistence.Id;
import java.io.Serializable;
import java.util.Objects;

public class QuoteHistoryStatsPK implements Serializable {
    private String startTime;
    private int quoteType;

    @Column(name = "start_time", nullable = false, length = 5)
    @Id
    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    @Column(name = "quote_type", nullable = false)
    @Id
    public int getQuoteType() {
        return quoteType;
    }

    public void setQuoteType(int quoteType) {
        this.quoteType = quoteType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QuoteHistoryStatsPK that = (QuoteHistoryStatsPK) o;
        return quoteType == that.quoteType &&
                Objects.equals(startTime, that.startTime);
    }

    @Override
    public int hashCode() {

        return Objects.hash(startTime, quoteType);
    }
}