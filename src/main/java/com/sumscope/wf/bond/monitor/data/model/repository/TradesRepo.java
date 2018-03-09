package com.sumscope.wf.bond.monitor.data.model.repository;

import com.sumscope.wf.bond.monitor.data.model.db.Trades;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.math.BigDecimal;
import java.util.List;

public interface TradesRepo extends JpaRepository<Trades, String> {
    List<Trades> findByYieldCurveAndCdcDiffValueGreaterThan(String bondCode, BigDecimal cdcDiffValue);

    @Query(value = "REPLACE INTO trades(bond_key,listed_market) VALUES('1','CIB')",nativeQuery = true)
    List<Trades> replaceInto(Trades trades);

    @Override
    void flush();
}
