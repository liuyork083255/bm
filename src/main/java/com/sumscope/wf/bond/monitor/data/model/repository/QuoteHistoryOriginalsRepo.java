package com.sumscope.wf.bond.monitor.data.model.repository;

import com.sumscope.wf.bond.monitor.data.model.db.QuoteHistoryOriginals;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

public interface QuoteHistoryOriginalsRepo extends JpaRepository<QuoteHistoryOriginals,Long>{
    @Modifying
    @Transactional
    @Query(value = "delete from quote_history_originals where DATE(quote_time) >= ?1 " +
            "and DATE(quote_time) <= ?1", nativeQuery = true)
    void deleteByDate(String date);
}
