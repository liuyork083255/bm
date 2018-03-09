package com.sumscope.wf.bond.monitor.data.access.cdh.util;

import com.alibaba.fastjson.JSONObject;
import com.sumscope.wf.bond.monitor.data.access.cdh.CdhBondGoodsGroupLoader;
import com.sumscope.wf.bond.monitor.data.access.cdh.CdhBondGroupRelationLoader;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class UserAttentionUtil {
    private static Map<String, List<String>> userAttentionList = new HashMap<>();
    @Autowired
    private CdhBondGoodsGroupLoader cdhBondGoodsGroupLoader;
    @Autowired
    private CdhBondGroupRelationLoader cdhBondGroupRelationLoader;

    /**
     * 首先根据用户 userId 查询用户的关注组
     * 然后根据每一个组的 id 查询组内的关注列表
     * @param userId
     * @return
     */
    public List<String> getUserAttentionList(String userId){
        List<String> list = new ArrayList<>();

        if(StringUtils.isBlank(userId)) return list;
        List<JSONObject> groupList = cdhBondGoodsGroupLoader.loadBondGoodsGroup(userId);
        if(CollectionUtils.isEmpty(groupList)) return list;

        groupList.forEach(gl -> {
            String groupId = gl.getString("id");
            List<JSONObject> attentionList = cdhBondGroupRelationLoader.loadBondGroupRelation(groupId);
            if(CollectionUtils.isEmpty(attentionList)) return;
            StringBuilder stringBuilder = new StringBuilder();
            attentionList.forEach(al -> {
                String bondKeyListMarket = stringBuilder.append(
                        al.getString("bond_key"))
                        .append("_")
                        .append(al.getString("market_id"))
                        .toString();
                list.add(bondKeyListMarket);
                stringBuilder.delete(0,stringBuilder.length());
            });
        });
        return list;
    }

    /**
     * 每晚定时清空 UserAttentionList 缓存
     * 由于考虑到关注列表并非本程序控制，所以缓存策略弃用
     */
    public void clearUserAttentionList(){
        synchronized (this) {
            userAttentionList.clear();
        }
    }
}
