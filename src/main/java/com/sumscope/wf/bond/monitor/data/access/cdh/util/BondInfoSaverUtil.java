package com.sumscope.wf.bond.monitor.data.access.cdh.util;

import com.sumscope.wf.bond.monitor.data.model.db.BondInfos;
import com.sumscope.wf.bond.monitor.data.model.repository.BondInfosRepo;
import com.sumscope.wf.bond.monitor.global.enums.SaveDBTypeEnum;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

@Component
public class BondInfoSaverUtil {

    @Autowired
    private JdbcUtil jdbcUtil;
    @Autowired
    private BondInfosRepo bondInfosRepo;

    public void saveBondInfo(List<BondInfos> list, SaveDBTypeEnum typeEnum){
        if(CollectionUtils.isEmpty(list)) return;

        if(SaveDBTypeEnum.JDBC.equal(typeEnum)){
            List<String> sqlList = convertToSqlStr(list);
            jdbcUtil.save(sqlList);
        } else {
            bondInfosRepo.save(list);
        }
    }

    private List<String> convertToSqlStr(List<BondInfos> dataList){
        List<String> list = new ArrayList<>();
        if(CollectionUtils.isEmpty(dataList)) return list;
        dataList.forEach(bi -> {
            String insertSQL = String.format("INSERT INTO `bond_infos` (`bond_key`,`listed_market`," +
                            "`yield_curve_code`,`category`,`sub_category`,`rating`,`issuer_code`," +
                            "`bond_type`,`issuer_name`,`short_name`,`bond_sub_type`,`is_municipal`," +
                            "`listed_type`,`institution_subtype`,`cbrc_financing_platform`,`province`) VALUES" +
                            "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    bi.getBondKey() == null ? null : "'"+bi.getBondKey()+"'",
                    bi.getListedMarket() == null ? null : "'"+bi.getListedMarket()+"'",
                    bi.getYieldCurveCode() == null ? null : "'"+bi.getYieldCurveCode()+"'",
                    bi.getCategory() == null ? null : "'"+bi.getCategory()+"'",
                    bi.getSubCategory() == null ? null : "'"+bi.getSubCategory()+"'",
                    bi.getRating() == null ? null : "'"+bi.getRating()+"'",
                    bi.getIssuerCode() == null ? null : "'"+bi.getIssuerCode()+"'",
                    bi.getBondType() == null ? null : "'"+bi.getBondType()+"'",
                    bi.getIssuerName() == null ? null : "'"+bi.getIssuerName()+"'",
                    bi.getShortName() == null ? null : "'"+bi.getShortName()+"'",
                    bi.getBondSubType() == null ? null : "'"+bi.getBondSubType()+"'",
                    bi.getIsMunicipal() == null ? null : "'"+bi.getIsMunicipal()+"'",
                    bi.getListedType() == null ? null : "'"+bi.getListedType()+"'",
                    bi.getInstitutionSubtype() == null ? null : "'"+bi.getInstitutionSubtype()+"'",
                    bi.getCbrcFinancingPlatform() == null ? null : "'"+bi.getCbrcFinancingPlatform()+"'",
                    bi.getProvince() == null ? null : "'"+bi.getProvince()+"'"
            );
            list.add(insertSQL);
        });
        return list;
    }
}
