package com.sumscope.wf.bond.monitor.data.model.db;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import java.sql.Time;
import java.util.Objects;

@Entity
public class Alerts {
    private long autoId;
    private String id;
    private String description;
    private Time updateTime;
    private String name;
    private String bondType;
    private String issuerName;
    private String alertType;
    private String bondKeyListedMarket;


    @Column(name = "auto_id", nullable = false)
    public long getAutoId() {
        return autoId;
    }

    public void setAutoId(long autoId) {
        this.autoId = autoId;
    }
    @Id
    @Column(name = "id", nullable = false, length = 64)
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Basic
    @Column(name = "description", nullable = true, length = 32)
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
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
    @Column(name = "name", nullable = true, length = 20)
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Basic
    @Column(name = "bond_type", nullable = true, length = 4)
    public String getBondType() {
        return bondType;
    }

    public void setBondType(String bondType) {
        this.bondType = bondType;
    }

    @Basic
    @Column(name = "issuer_name", nullable = true, length = 32)
    public String getIssuerName() {
        return issuerName;
    }

    public void setIssuerName(String issuerName) {
        this.issuerName = issuerName;
    }

    @Basic
    @Column(name = "alert_type", nullable = true, length = 2)
    public String getAlertType() {
        return alertType;
    }

    public void setAlertType(String alertType) {
        this.alertType = alertType;
    }

    @Basic
    @Column(name = "bond_key_listed_market", nullable = true, length = 32)
    public String getBondKeyListedMarket() {
        return bondKeyListedMarket;
    }

    public void setBondKeyListedMarket(String bondKeyListedMarket) {
        this.bondKeyListedMarket = bondKeyListedMarket;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Alerts alerts = (Alerts) o;
        return autoId == alerts.autoId &&
                Objects.equals(id, alerts.id) &&
                Objects.equals(description, alerts.description) &&
                Objects.equals(updateTime, alerts.updateTime) &&
                Objects.equals(name, alerts.name) &&
                Objects.equals(bondType, alerts.bondType) &&
                Objects.equals(issuerName, alerts.issuerName) &&
                Objects.equals(alertType, alerts.alertType) &&
                Objects.equals(bondKeyListedMarket, alerts.bondKeyListedMarket);
    }

    @Override
    public int hashCode() {

        return Objects.hash(autoId, id, description, updateTime, name, bondType, issuerName, alertType, bondKeyListedMarket);
    }
}
