package com.sumscope.wf.bond.monitor.data.model.db;

import javax.persistence.Column;
import javax.persistence.Id;
import java.io.Serializable;

public class BondInfosPK implements Serializable {
    private String bondKey;
    private String listedMarket;

    @Column(name = "bond_key", nullable = false, length = 100)
    @Id
    public String getBondKey() {
        return bondKey;
    }

    public void setBondKey(String bondKey) {
        this.bondKey = bondKey;
    }

    @Column(name = "listed_market", nullable = false, length = 100)
    @Id
    public String getListedMarket() {
        return listedMarket;
    }

    public void setListedMarket(String listedMarket) {
        this.listedMarket = listedMarket;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BondInfosPK that = (BondInfosPK) o;

        if (bondKey != null ? !bondKey.equals(that.bondKey) : that.bondKey != null) return false;
        if (listedMarket != null ? !listedMarket.equals(that.listedMarket) : that.listedMarket != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = bondKey != null ? bondKey.hashCode() : 0;
        result = 31 * result + (listedMarket != null ? listedMarket.hashCode() : 0);
        return result;
    }
}
