package com.sumscope.wf.bond.monitor.data.model.repository;

import com.sumscope.wf.bond.monitor.data.model.db.BondInfos;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import javax.transaction.Transactional;

public interface BondInfosRepo extends JpaRepository<BondInfos,Long>{
    @Modifying
    @Transactional
    @Query(value = "TRUNCATE table bond_infos",nativeQuery = true)
    void deleteAll0();
}
