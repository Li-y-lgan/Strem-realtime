package com.reatime.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

/**
 * @Package com.label.func.IntervalDbOrderInfoJoinOrderDetailProcessFunc
 * @Author li.yan
 * @Date 2025/5/14 21:38
 * @description: 该类用于将订单信息和订单详情数据进行连接操作。
 */
public class IntervalDbOrderInfoJoinOrderDetailProcessFunc extends ProcessJoinFunction<JSONObject, JSONObject, JSONObject> {
    @Override
    public void processElement(JSONObject jsonObject1, JSONObject jsonObject2, Context context, Collector<JSONObject> collector) {
        // 创建一个新的 JSONObject 用于存储连接结果
        JSONObject result = new JSONObject();
        // 将订单信息添加到结果中
        result.putAll(jsonObject1);
        // 将订单详情中的商品数量添加到结果中
        result.put("sku_num", jsonObject2.getLongValue("sku_num"));
        // 将订单详情中的优惠券分摊金额添加到结果中
        result.put("split_coupon_amount", jsonObject2.getString("sku_num"));

        result.put("sku_name", jsonObject2.getString("sku_name"));
        // 将订单详情中的订单价格添加到结果中
        result.put("order_price", jsonObject2.getString("order_price"));
        // 将订单详情中的详情 ID 添加到结果中
        result.put("detail_id", jsonObject2.getString("id"));
        // 将订单详情中的订单 ID 添加到结果中
        result.put("order_id", jsonObject2.getString("order_id"));
        // 将订单详情中的商品 ID 添加到结果中
        result.put("sku_id", jsonObject2.getLongValue("sku_id"));
        // 将订单详情中的活动分摊金额添加到结果中
        result.put("split_activity_amount", jsonObject2.getDoubleValue("split_activity_amount"));
        // 将订单详情中的总分摊金额添加到结果中
        result.put("split_total_amount", jsonObject2.getDoubleValue("split_total_amount"));
        // 输出连接结果
        collector.collect(result);
    }
}