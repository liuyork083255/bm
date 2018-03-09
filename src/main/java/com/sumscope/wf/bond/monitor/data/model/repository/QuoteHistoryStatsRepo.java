package com.sumscope.wf.bond.monitor.data.model.repository;

import com.sumscope.wf.bond.monitor.data.model.db.QuoteHistoryStats;
import org.springframework.data.jpa.repository.JpaRepository;

public interface QuoteHistoryStatsRepo extends JpaRepository<QuoteHistoryStats,Long> {
}
