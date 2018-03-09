package com.sumscope.wf.bond.monitor.service.converter;

import com.sumscope.wf.bond.monitor.data.access.cdh.util.UserAttentionUtil;
import com.sumscope.wf.bond.monitor.data.model.db.TradeStats;
import com.sumscope.wf.bond.monitor.data.model.db.Trades;
import com.sumscope.wf.bond.monitor.domain.monitor.BondInfoDTO;
import com.sumscope.wf.bond.monitor.domain.monitor.MonitorListDTO;
import com.sumscope.wf.bond.monitor.domain.monitor.MonitorListItemDTO;
import com.sumscope.wf.bond.monitor.domain.monitor.MonitorPlotDTO;
import com.sumscope.wf.bond.monitor.global.constants.BondMonitorConstants;
import com.sumscope.wf.bond.monitor.utils.BusinessUtils;
import com.sumscope.wf.bond.monitor.utils.DateUtils;
import com.sumscope.wf.bond.monitor.web.condition.MonitorListCondition;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class MonitorConverter {
    @Autowired
    private UserAttentionUtil userAttentionUtil;

    public List<MonitorPlotDTO> buildMonitorPlot(List<Trades> trades, List<String> attentions) {
        List<MonitorPlotDTO> monitorPlotDTOS = new ArrayList<>();
        for(Trades trade : trades){
            boolean isFollow = false;
            MonitorPlotDTO monitorPlotDTO = new MonitorPlotDTO();
            monitorPlotDTO.setBondCode(trade.getBondCode());
            monitorPlotDTO.setTime(DateUtils.convertTimeToString(trade.getUpdateTime()));
            monitorPlotDTO.setName(trade.getShortName());
            monitorPlotDTO.setPrice(trade.getDealPriceValue());
            monitorPlotDTO.setDiff(trade.getCdcDiffValue());
            monitorPlotDTO.setTerm(trade.getRemainTime());
            monitorPlotDTO.setYieldCurve(trade.getYieldCurve());
            if (attentions.contains(trade.getBondKey() + "_" + trade.getListedMarket())){
                isFollow = true;
            }
            monitorPlotDTO.setIsFollow(isFollow);
            monitorPlotDTOS.add(monitorPlotDTO);
        }
        return monitorPlotDTOS;
    }

    public MonitorListDTO buildMonitorList(Page<TradeStats> allTradeStats, MonitorListCondition condition) {
        MonitorListDTO monitorListDTO = new MonitorListDTO();
        List<MonitorListItemDTO> itemDtos = new ArrayList<>();
        for (TradeStats tradeStat : allTradeStats.getContent()) {
            MonitorListItemDTO itemDto = new MonitorListItemDTO();
            itemDto.setBondCode(tradeStat.getBondCode());
            itemDto.setShortName(tradeStat.getShortName());
            itemDto.setLatestPrice(tradeStat.getLatestPrice());
            itemDto.setCdcPrice(tradeStat.getCdcPrice());
            itemDto.setCurDiff(tradeStat.getLatestPriceDiff());
            itemDto.setTodayAvgDiff(tradeStat.getAvgDiffPrice());
            itemDto.setTodayDiffNum(BusinessUtils.getDiffValue(condition.getDiff(), tradeStat));
            itemDto.setTodayDealNum(tradeStat.getTradeCount());
            itemDto.setUpdateTime(DateUtils.convertTimeToString(tradeStat.getUpdateTime()));
            itemDtos.add(itemDto);
        }
        monitorListDTO.setData(itemDtos);
        monitorListDTO.setTotal((int)allTradeStats.getTotalElements());
        return monitorListDTO;
    }

    public BondInfoDTO buildBondInfo(TradeStats tradeStats, String bondCode, String userId, String diff) {
        BondInfoDTO bondInfoDTO = new BondInfoDTO();
        bondInfoDTO.setBondCode(bondCode);
        bondInfoDTO.setShortName(tradeStats.getShortName());
        bondInfoDTO.setLatestPrice(tradeStats.getLatestPrice());
        bondInfoDTO.setCdcPrice(tradeStats.getCdcPrice());
        bondInfoDTO.setCurDiff(tradeStats.getLatestPriceDiff());
        bondInfoDTO.setTodayAvgDiff(tradeStats.getAvgDiffPrice());
        bondInfoDTO.setTodayDealNum(tradeStats.getTradeCount());
        bondInfoDTO.setUpdateTime(DateUtils.convertTimeToString(tradeStats.getUpdateTime()));
        if (StringUtils.isEmpty(diff)) {
            Map<String, Integer> diffNum = new HashMap<>();
            diffNum.put(BondMonitorConstants.DIFF_FIVE, tradeStats.getPriceDiff1Count());
            diffNum.put(BondMonitorConstants.DIFF_TEN, tradeStats.getPriceDiff2Count());
            diffNum.put(BondMonitorConstants.DIFF_FIFTEEN, tradeStats.getPriceDiff3Count());
            diffNum.put(BondMonitorConstants.DIFF_TWENTY, tradeStats.getPriceDiff4Count());
            diffNum.put(BondMonitorConstants.DIFF_TWENTY_FIVE, tradeStats.getPriceDiff5Count());
            bondInfoDTO.setTodayDiffNum(diffNum);
        } else {
            bondInfoDTO.setTodayDiffNum(BusinessUtils.getDiffValue(diff, tradeStats));
        }
        List<String> attentions = userAttentionUtil.getUserAttentionList(userId);
        String key = tradeStats.getBondKey() + "_" + tradeStats.getListedMarket();
        bondInfoDTO.setYieldCurve(tradeStats.getYieldCurve());
        bondInfoDTO.setIsFollow(attentions.contains(key));
        return bondInfoDTO;
    }
}
