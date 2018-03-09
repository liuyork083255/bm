package com.sumscope.wf.bond.monitor.data.model.db;

import javax.persistence.*;
import java.sql.Date;
import java.util.Objects;

@Entity
@Table(name = "quote_days", schema = "wf_bond_monitor", catalog = "")
public class QuoteDays {
    private long autoId;
    private Date day;
    private String startTime;
    private Integer quoteType;
    private Integer total;

    @Id
    @Column(name = "auto_id", nullable = false)
    public long getAutoId() {
        return autoId;
    }

    public void setAutoId(long autoId) {
        this.autoId = autoId;
    }

    @Basic
    @Column(name = "day", nullable = true)
    public Date getDay() {
        return day;
    }

    public void setDay(Date day) {
        this.day = day;
    }

    @Basic
    @Column(name = "start_time", nullable = true, length = 5)
    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
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
    @Column(name = "total", nullable = true)
    public Integer getTotal() {
        return total;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QuoteDays quoteDays = (QuoteDays) o;
        return autoId == quoteDays.autoId &&
                Objects.equals(day, quoteDays.day) &&
                Objects.equals(startTime, quoteDays.startTime) &&
                Objects.equals(quoteType, quoteDays.quoteType) &&
                Objects.equals(total, quoteDays.total);
    }

    @Override
    public int hashCode() {

        return Objects.hash(autoId, day, startTime, quoteType, total);
    }
}
