package com.sumscope.wf.bond.monitor.data.model.db;

import javax.persistence.Column;
import javax.persistence.Id;
import java.io.Serializable;
import java.util.Objects;

public class QuotesPK implements Serializable {
    private String id;
    private int quoteType;

    @Column(name = "id", nullable = false, length = 32)
    @Id
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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
        QuotesPK quotesPK = (QuotesPK) o;
        return quoteType == quotesPK.quoteType &&
                Objects.equals(id, quotesPK.id);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, quoteType);
    }
}