package com.sumscope.wf.bond.monitor.data.model.repository;

import com.sumscope.wf.bond.monitor.data.model.db.YieldCurves;
import org.springframework.data.jpa.repository.JpaRepository;

public interface YieldCurvesRepo extends JpaRepository<YieldCurves,String> {
}
