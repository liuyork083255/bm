package com.sumscope.wf.bond.monitor.data.access.cdh.util;

import com.alibaba.fastjson.JSONObject;
import com.sumscope.wf.bond.monitor.data.access.cdh.*;
import com.sumscope.wf.bond.monitor.data.model.db.BondInfos;
import com.sumscope.wf.bond.monitor.data.model.repository.BondInfosRepo;
import com.sumscope.wf.bond.monitor.global.constants.BondMonitorConstants;
import com.sumscope.wf.bond.monitor.global.enums.SaveDBTypeEnum;
import com.sumscope.wf.bond.monitor.global.exception.BondMonitorError;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class BondInfoLoaderUtil {
    private static final Logger logger = LoggerFactory.getLogger(BondInfoLoaderUtil.class);

    @Autowired
    private BondInfosRepo bondInfosRepo;
    @Autowired
    private CdhBondLoader cdhBondLoader;
    @Autowired
    private CdhBondListInfoLoader cdhBondListInfoLoader;
    @Autowired
    private BondInfoSaverUtil bondInfoSaverUtil;
    @Autowired
    private CdhBeforeWorkdayLoader cdhBeforeWorkdayLoader;
    @Autowired
    private CdhCdcBondValuationLoader cdhCdcBondValuationLoader;
    @Autowired
    private CdhPIssuerInfoLoader cdhPIssuerInfoLoader;
    @Autowired
    private CdhInstitutionLoader cdhInstitutionLoader;

    /**
     * 程序启动第一次全量加载 bond info 信息，并保存 DB
     * 默认加载类型 jdbc
     */
    public void loadBondInfoFirstStart(){
        // 从 p_bond_list_info 获取全量 bondKey listedMarket
        // 因为 bond 里面只有 bondKey 字段，没有区分市场
        List<JSONObject> bondListInfoList = cdhBondListInfoLoader.loadCdhBondListInfo("");
        if(CollectionUtils.isEmpty(bondListInfoList)) throw BondMonitorError.InitFailed;

        // 根据 List<JSONObject> 生成所有 DB BondInfo model
        List<BondInfos> allBondInfoByApi = getAllBondInfoByApi(bondListInfoList);

        // 先后根据 bondKey listedMarket 分组
        Map<String, Map<String, BondInfos>> globalBondInfoMap = groupBondInfoListByListedMarket(allBondInfoByApi);

        /* 属性注入 api：cdc_bond_valuation、bond、p_issuer_info、institution、webbond_bond */
        // 1 cdc_bond_valuation 评级属性注入
        cdhCdcBondValuationLoader.attributeInject(globalBondInfoMap);
        // 2 bond 属性注入      第一次查询 bond 条件为全量
        cdhBondLoader.attributeInject(globalBondInfoMap,"");

        // 根据 issuerCode 分组
        Map<String, List<BondInfos>> stringListMap = groupBondInfoListByIssuerCode(allBondInfoByApi);

        // 3 p_issuer_info 属性注入
        cdhPIssuerInfoLoader.attributeInject(stringListMap,"");
        // 4 institution 属性注入
        cdhInstitutionLoader.attributeInject(stringListMap,"");
        // 5 webbond_bond 属性注入
        // TODO: 2018/3/8
        // 原来是从该表中获取 bondType 字段，后来可以根据 bond 表中字段判断出是信用债还是利率债

        // insert DB
        bondInfoSaverUtil.saveBondInfo(allBondInfoByApi,SaveDBTypeEnum.JDBC);
    }

    /**
     * 定时任务，更新 bond_infos 表，测试下来，如果是采用增量更新，则必须使用 JPA，插入速度非常慢，
     * 为了使用 JDBC 写入，则采用全量更新，故首先是删除表所有数据，然后全量加载所有数据
     * 测试下来，使用 JDBC 全量加载方式反而更快，数据库压力减小
     */
    public void updateBondInfoTableByDay(){
        bondInfosRepo.deleteAll0();
        loadBondInfoFirstStart();
    }


    /**
     * 该方法根据全量 p_bond_list_info 数据生成所有的 bond_info DB model，
     * 并且将注入 model 的 bond_key、listed_market
     * @param list
     */
    private List<BondInfos> getAllBondInfoByApi(List<JSONObject> list){
        List<BondInfos> bondInfoList = new ArrayList<>();
        list.forEach(jo -> {
            BondInfos bi = new BondInfos();
            String bondKey = jo.getString(BondMonitorConstants.BOND_KEY);
            String listedMarket = jo.getString(BondMonitorConstants.LISTED_MARKET);
            // 防止 p_bond_list_info 中数据非法，直接忽略
            if(StringUtils.isBlank(bondKey) || StringUtils.isBlank(listedMarket)) return;

            bi.setBondKey(bondKey);
            bi.setListedMarket(listedMarket);
            bi.setIssuerCode(""); // 这里初始化为空字符串，防止后面请求数据该字段为null，出现分组异常
            bondInfoList.add(bi);
        });
        return bondInfoList;
    }

    /**
     * 首先根据 bondKey 分组，然后根据 listedMarket 分组，便于后续属性注入
     * {
     *     "bondKey" : {
     *         "listedMarket" : BondInfo
     *     }
     * }
     * @param bondInfoList
     * @return
     */
    private Map<String,Map<String,BondInfos>> groupBondInfoListByListedMarket(List<BondInfos> bondInfoList){
        Map<String,Map<String,BondInfos>> globalBondInfoMap = new HashMap<>();

        Map<String, List<BondInfos>> bondKeyMap
                = bondInfoList.stream().collect(Collectors.groupingBy(BondInfos::getBondKey));

        bondKeyMap.forEach((k,v) -> {
            if(CollectionUtils.isEmpty(v) || StringUtils.isBlank(k)) return;
            globalBondInfoMap.put(k, v.stream().collect(Collectors.toMap(BondInfos::getListedMarket, bi -> bi)));
        });
        return globalBondInfoMap;
    }

    /**
     * 根据 {@link BondInfos#issuerCode} 分组 ，便于后续属性注入
     * {
     *     "issuerCode":[BondInfo1,BondInfo2 ...]
     * }
     * @param bondInfoList
     */
    private  Map<String, List<BondInfos>> groupBondInfoListByIssuerCode(List<BondInfos> bondInfoList){
        return bondInfoList.stream().collect(Collectors.groupingBy(BondInfos::getIssuerCode));
    }

}
