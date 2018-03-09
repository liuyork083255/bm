package com.sumscope.wf.bond.monitor.data.model.repository;

import com.sumscope.wf.bond.monitor.data.model.db.TradeStats;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface TradeStatsRepo extends JpaRepository<TradeStats, Long> {

    TradeStats getByBondCode(String bondCode);

    @Query(value = "Select * from trade_stats where yield_curve = ?1 and price_diff1_count > 0 ORDER BY ?#{#pageable}",
            countQuery = "SELECT count(*) FROM trade_stats WHERE yield_curve = ?1 and price_diff1_count > 0",
            nativeQuery = true)
    Page<TradeStats> findByYieldCurve(String yieldCurve ,Pageable pageable);

    @Query(value = "Select * from trade_stats where yield_curve = ?1 and price_diff2_count > 0 ORDER BY ?#{#pageable}",
            countQuery = "SELECT count(*) FROM trade_stats WHERE yield_curve = ?1 and price_diff2_count > 0",
            nativeQuery = true)
    Page<TradeStats> findByYieldCurveAndDiff2(String yieldCurve ,Pageable pageable);
    @Query(value = "Select * from trade_stats where yield_curve = ?1 and price_diff3_count > 0 ORDER BY ?#{#pageable}",
            countQuery = "SELECT count(*) FROM trade_stats WHERE yield_curve = ?1 and price_diff3_count > 0",
            nativeQuery = true)
    Page<TradeStats> findByYieldCurveAndDiff3(String yieldCurve ,Pageable pageable);
    @Query(value = "Select * from trade_stats where yield_curve = ?1 and price_diff4_count > 0 ORDER BY ?#{#pageable}",
            countQuery = "SELECT count(*) FROM trade_stats WHERE yield_curve = ?1 and price_diff4_count > 0",
            nativeQuery = true)
    Page<TradeStats> findByYieldCurveAndDiff4(String yieldCurve ,Pageable pageable);
    @Query(value = "Select * from trade_stats where yield_curve = ?1 and price_diff5_count > 0 ORDER BY ?#{#pageable}",
            countQuery = "SELECT count(*) FROM trade_stats WHERE yield_curve = ?1 and price_diff5_count > 0",
            nativeQuery = true)
    Page<TradeStats> findByYieldCurveAndDiff5(String yieldCurve ,Pageable pageable);



}
