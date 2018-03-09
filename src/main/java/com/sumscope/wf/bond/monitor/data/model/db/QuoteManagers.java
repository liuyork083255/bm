package com.sumscope.wf.bond.monitor.data.model.db;

import javax.persistence.*;
import java.util.Arrays;
import java.util.Objects;

@Entity
@Table(name = "quote_managers", schema = "wf_bond_monitor", catalog = "")
public class QuoteManagers {
    private long autoId;
    private String day;
    private byte[] ranges;
    private byte[] hotMaps;
    private byte[] accQuoteMaps;
    private byte[] accAlertMaps;


    @Column(name = "auto_id", nullable = false)
    public long getAutoId() {
        return autoId;
    }

    public void setAutoId(long autoId) {
        this.autoId = autoId;
    }

    @Id
    @Column(name = "day", nullable = false, length = 100)
    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    @Basic
    @Column(name = "ranges", nullable = true)
    public byte[] getRanges() {
        return ranges;
    }

    public void setRanges(byte[] ranges) {
        this.ranges = ranges;
    }

    @Basic
    @Column(name = "hot_maps", nullable = true)
    public byte[] getHotMaps() {
        return hotMaps;
    }

    public void setHotMaps(byte[] hotMaps) {
        this.hotMaps = hotMaps;
    }

    @Basic
    @Column(name = "acc_quote_maps", nullable = true)
    public byte[] getAccQuoteMaps() {
        return accQuoteMaps;
    }

    public void setAccQuoteMaps(byte[] accQuoteMaps) {
        this.accQuoteMaps = accQuoteMaps;
    }

    @Basic
    @Column(name = "acc_alert_maps", nullable = true)
    public byte[] getAccAlertMaps() {
        return accAlertMaps;
    }

    public void setAccAlertMaps(byte[] accAlertMaps) {
        this.accAlertMaps = accAlertMaps;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QuoteManagers that = (QuoteManagers) o;
        return autoId == that.autoId &&
                Objects.equals(day, that.day) &&
                Arrays.equals(ranges, that.ranges) &&
                Arrays.equals(hotMaps, that.hotMaps) &&
                Arrays.equals(accQuoteMaps, that.accQuoteMaps) &&
                Arrays.equals(accAlertMaps, that.accAlertMaps);
    }

    @Override
    public int hashCode() {

        int result = Objects.hash(autoId, day);
        result = 31 * result + Arrays.hashCode(ranges);
        result = 31 * result + Arrays.hashCode(hotMaps);
        result = 31 * result + Arrays.hashCode(accQuoteMaps);
        result = 31 * result + Arrays.hashCode(accAlertMaps);
        return result;
    }
}
