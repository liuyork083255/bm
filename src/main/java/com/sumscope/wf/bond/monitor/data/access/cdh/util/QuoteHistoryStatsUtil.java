package com.sumscope.wf.bond.monitor.data.access.cdh.util;

import com.alibaba.fastjson.JSON;
import com.sumscope.wf.bond.monitor.data.model.db.QuoteHistories;
import com.sumscope.wf.bond.monitor.data.model.db.QuoteHistoryStats;
import com.sumscope.wf.bond.monitor.global.ConfigParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.math.BigDecimal;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Component
public class QuoteHistoryStatsUtil {
    private static final Logger logger = LoggerFactory.getLogger(QuoteHistoryStatsUtil.class);
    private StringBuilder stringBuilder = new StringBuilder();
    private BigDecimal big5Decimal;
    private List<String> intervalList = new ArrayList<>();
    private Map<String,Integer> intervalMap = new HashMap<>();

    @Autowired
    private ConfigParams configParams;

    /**
     * 1 加载 bond 活跃时间区间 和 间隔时间
     *
     * 2 加载 quote 历史表缓存天数周期
     * 默认加载 5 天
     */
    @PostConstruct
    public void init(){
        // 加载 bond 活跃时间区间 和 间隔时间，并生成所有 list 区间
        LocalTime start = configParams.getRangeStartTime();
        LocalTime end = configParams.getRangeEndTime();
        Integer intervalMin = configParams.getIntervalMin();
        // 校验时间大小关系
        Assert.isTrue(start.isBefore(end),"start time must be less than end time");
        // 校验时间间隔是否合法
        Assert.isTrue(60 % intervalMin == 0,"interval must be divided by 60");

        // 初始化所有时间分段，并设置默认值 0
        while (start.isBefore(end)){
            String keyPre = start.format(DateTimeFormatter.ofPattern("HH:mm"));
            this.intervalMap.put(keyPre + "_0",0);
            this.intervalMap.put(keyPre + "_1",0);
            this.intervalMap.put(keyPre + "_2",0);
            this.intervalList.add(keyPre);
            start = start.plusMinutes(configParams.getIntervalMin());
        }
        String keyPre = start.format(DateTimeFormatter.ofPattern("HH:mm"));
        this.intervalMap.put(keyPre + "_0",0);
        this.intervalMap.put(keyPre + "_1",0);
        this.intervalMap.put(keyPre + "_2",0);
        this.intervalList.add(keyPre);
        logger.info("bond active range time : {}",intervalList);

        // 加载 quote 历史表缓存天数周期
        Integer loadQuoteHistoryDays = configParams.getLoadQuoteHistoryDays();
        logger.info("load quote_history_* tables's data life cycle is {}",loadQuoteHistoryDays);
        if(loadQuoteHistoryDays > 0)
            big5Decimal = new BigDecimal(loadQuoteHistoryDays);
        else
            big5Decimal = new BigDecimal(5);
    }

    public List<QuoteHistoryStats> convertToQuoteHistoryStats(List<QuoteHistories> dataList){
        List<QuoteHistories> filterResult = filterTime(dataList);
        List<QuoteHistoryStats> qhsList = new ArrayList<>();
        int interval = configParams.getIntervalMin();
        // 将所有行情按 HH:mm_x 格式分组 并统计个数
        Map<String, List<QuoteHistories>> map = filterResult.stream().collect(Collectors.groupingBy(qh -> {
            LocalTime localTime = qh.getQuoteTime().toLocalDateTime().toLocalTime();
            int hour = localTime.getHour();
            int minute = localTime.getMinute();
            int quoteType = qh.getQuoteType();
            return getHourMinuteKey(hour, minute, quoteType,interval);
        }));

        // 计算统计分组总数
        map.forEach((k,v) ->  intervalMap.put(k,v.size()) );

        // 将分组 map 转为 db model
        this.intervalMap.forEach((k,v) ->{
            QuoteHistoryStats qhs = new QuoteHistoryStats();
            String[] split = k.split("_");
            String time = split[0];
            String type = split[1];

            qhs.setStartTime(time);
            qhs.setQuoteType(Integer.parseInt(type));
            qhs.setTotal(calculateTotal(v));
            qhsList.add(qhs);
        });

        qhsList.sort(Comparator.comparing(QuoteHistoryStats::getStartTime)
                .thenComparing(QuoteHistoryStats::getQuoteType));

        return qhsList;
    }

    /**
     * 过滤所有行情 {@link QuoteHistories#quoteTime}
     * 不在 {@link ConfigParams#getRangeStartTime()} 和 {@link ConfigParams#getRangeEndTime()} 范围内
     * @param dataList
     * @return
     */
    private List<QuoteHistories> filterTime(List<QuoteHistories> dataList){
        if(CollectionUtils.isEmpty(dataList)) return dataList;
        logger.info("filter before quoteTime size={}",dataList.size());

        LocalTime start = configParams.getRangeStartTime().minusSeconds(1);
        LocalTime end = configParams.getRangeEndTime().plusSeconds(1);
        List<QuoteHistories> list = dataList.stream().filter(qh -> {
            LocalTime localTime;
            try {
                localTime = qh.getQuoteTime().toLocalDateTime().toLocalTime();
            } catch (Exception e) {
                logger.error("get quote time error.   model={}   exception={}", JSON.toJSONString(qh), e.getMessage());
                return false;
            }

            if (localTime.isAfter(start) && localTime.isBefore(end)) {
                return true;
            } else {
                logger.warn("quote time is out of range {}-{}.   msg={}",start,end, qh.getQuoteTime());
                return false;
            }
        }).collect(Collectors.toList());
        logger.info("filter after quoteTime size={}",list.size());
        return list;
    }

    /**
     *  该方法局限于整点可被60整除的时间间隔 1,2,3,5,,6,10 min，默认是5
     *  如果是每次间隔时间为 7 min，则无法智能判断正确区间
     * @param hour
     * @param minute
     * @param quoteType
     * @param interval 时间间隔 1,2,3,5,,6,10 min
     * @return
     */
    public String getHourMinuteKey(int hour,int minute,int quoteType,int interval){
        stringBuilder.delete(0, stringBuilder.length());
        minute = (minute / interval) * interval;

        if(hour < 10) stringBuilder.append("0").append(hour).append(":");
        else stringBuilder.append(hour).append(":");
        if(minute < 10) stringBuilder.append("0").append(minute);
        else stringBuilder.append(minute);

        stringBuilder.append("_").append(quoteType);
        return stringBuilder.toString();
    }

    /**
     * 该方法是 根据{@link ConfigParams#loadQuoteHistoryDays}配置 计算 total 值
     * @param total
     * @return
     */
    private BigDecimal calculateTotal(int total){
        BigDecimal bigDecimal = new BigDecimal(total).setScale(2);
        return bigDecimal.divide(big5Decimal,3,BigDecimal.ROUND_DOWN);
    }

}
