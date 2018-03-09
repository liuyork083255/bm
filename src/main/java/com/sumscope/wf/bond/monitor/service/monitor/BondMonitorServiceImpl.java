package com.sumscope.wf.bond.monitor.service.monitor;

import com.sumscope.wf.bond.monitor.data.access.cdh.CdhYieldCurveLoader;
import com.sumscope.wf.bond.monitor.data.access.cdh.util.UserAttentionUtil;
import com.sumscope.wf.bond.monitor.data.model.cache.YieldCurveModel;
import com.sumscope.wf.bond.monitor.data.model.db.TradeStats;
import com.sumscope.wf.bond.monitor.data.model.db.Trades;
import com.sumscope.wf.bond.monitor.data.model.db.YieldCurves;
import com.sumscope.wf.bond.monitor.data.model.repository.TradeStatsRepo;
import com.sumscope.wf.bond.monitor.data.model.repository.TradesRepo;
import com.sumscope.wf.bond.monitor.data.model.repository.YieldCurvesRepo;
import com.sumscope.wf.bond.monitor.global.constants.BondMonitorConstants;
import com.sumscope.wf.bond.monitor.domain.monitor.BondInfoDTO;
import com.sumscope.wf.bond.monitor.domain.monitor.CdcCurveDTO;
import com.sumscope.wf.bond.monitor.domain.monitor.MonitorListDTO;
import com.sumscope.wf.bond.monitor.domain.monitor.MonitorPlotDTO;
import com.sumscope.wf.bond.monitor.global.enums.DiffValueFieldEnum;
import com.sumscope.wf.bond.monitor.global.exception.BondMonitorError;
import com.sumscope.wf.bond.monitor.service.converter.MonitorConverter;
import com.sumscope.wf.bond.monitor.utils.converter.DbToDtoConverter;
import com.sumscope.wf.bond.monitor.web.condition.MonitorListCondition;
import com.sumscope.wf.bond.monitor.web.condition.MonitorPlotCondition;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

@Service
public class BondMonitorServiceImpl implements BondMonitorServiceI {

    private static final Logger logger = LoggerFactory.getLogger(BondMonitorServiceImpl.class);
    @Autowired
    private YieldCurvesRepo yieldCurvesRepo;
    @Autowired
    private TradeStatsRepo tradeStatsRepo;
    @Autowired
    private DbToDtoConverter dbToDtoConverter;
    @Autowired
    private CdhYieldCurveLoader cdhYieldCurveLoader;
    @Autowired
    private MonitorConverter monitorConverter;
    @Autowired
    private TradesRepo tradesRepo;
    @Autowired
    private UserAttentionUtil userAttentionUtil;

    private List<YieldCurves> yieldCurvesCache = new ArrayList<>();

    @Override
    public List<String> getDiffValue() {
        return DiffValueFieldEnum.toList();
    }

    @Override
    public List<YieldCurves> getCurveInfo() {
        if(CollectionUtils.isEmpty(yieldCurvesCache)){
            try {
                this.yieldCurvesCache = yieldCurvesRepo.findAll();
                return this.yieldCurvesCache;
            } catch (Exception e) {
                logger.error("find db yield_curves error.    exception:{}", e.getMessage());
                throw BondMonitorError.FindDBFailed;
            }
        }
        return this.yieldCurvesCache;
    }

    @Override
    public CdcCurveDTO getCdcCurve(String curveCode) {
        CdcCurveDTO cdcCurveDTO = new CdcCurveDTO();
        YieldCurveModel yieldCurve = cdhYieldCurveLoader.getYieldCurve(curveCode);
        cdcCurveDTO.setPeriod(yieldCurve.getPeriod());
        cdcCurveDTO.setYield(yieldCurve.getYield());
        return cdcCurveDTO;
    }

    @Override
    public List<MonitorPlotDTO> getMonitorPlot(MonitorPlotCondition condition) {
        List<Trades> trades = tradesRepo.findByYieldCurveAndCdcDiffValueGreaterThan(
                condition.getCurveCode(), condition.getDiff());
        List<String> attentions = userAttentionUtil.getUserAttentionList(condition.getUserId());
        return monitorConverter.buildMonitorPlot(trades, attentions);
    }

    @Override
    public MonitorListDTO getMonitorList(MonitorListCondition condition) {
        Sort.Direction sort = StringUtils.equalsIgnoreCase("asc", condition.getSort()) ? Sort.Direction.ASC :
                Sort.Direction.DESC;
        String orderBy = condition.getOrderBy();
        if ("price_diff_count".equalsIgnoreCase(orderBy)){
            orderBy = DiffValueFieldEnum.getDiffField(condition.getDiff());
        }
        Pageable pageable = new PageRequest(condition.getOffset(), condition.getLimit(), new Sort(
                new Sort.Order(sort, orderBy)
        ));
        Page<TradeStats> tradeStats = null;
        switch (condition.getDiff()) {
            case BondMonitorConstants.DIFF_FIVE:
                tradeStats = tradeStatsRepo.findByYieldCurve(condition.getCurveCode(), pageable);
                break;
            case BondMonitorConstants.DIFF_TEN:
                tradeStats = tradeStatsRepo.findByYieldCurveAndDiff2(condition.getCurveCode(), pageable);
                break;
            case BondMonitorConstants.DIFF_FIFTEEN:
                tradeStats = tradeStatsRepo.findByYieldCurveAndDiff3(condition.getCurveCode(), pageable);
                break;
            case BondMonitorConstants.DIFF_TWENTY:
                tradeStats = tradeStatsRepo.findByYieldCurveAndDiff4(condition.getCurveCode(), pageable);
                break;
            case BondMonitorConstants.DIFF_TWENTY_FIVE:
                tradeStats = tradeStatsRepo.findByYieldCurveAndDiff5(condition.getCurveCode(), pageable);
                break;
            default:
                logger.error("the diffValue {} not in support list", condition.getDiff());
        }
        return monitorConverter.buildMonitorList(tradeStats, condition);
    }

    @Override
    public BondInfoDTO getBondInfoByCode(String bondCode, String diff, String userId) {
        TradeStats tradeStats = tradeStatsRepo.getByBondCode(bondCode);
        return monitorConverter.buildBondInfo(tradeStats, bondCode, userId, diff);
    }

}
