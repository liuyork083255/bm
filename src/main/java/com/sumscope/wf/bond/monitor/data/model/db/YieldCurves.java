package com.sumscope.wf.bond.monitor.data.model.db;

import javax.persistence.*;
import java.util.Objects;

@Entity
@Table(name = "yield_curves", schema = "wf_bond_monitor", catalog = "")
public class YieldCurves {
    private String code;
    private String name;

    @Id
    @Column(name = "code", nullable = false, length = 30)
    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    @Basic
    @Column(name = "name", nullable = true, length = 32)
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        YieldCurves that = (YieldCurves) o;
        return Objects.equals(code, that.code) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {

        return Objects.hash(code, name);
    }
}
