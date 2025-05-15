package com.reatime.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;

/**
 * @Package com.label.func.MapOrderInfoData
 * @Author li.yan
 * @Date 2025/5/14 14:10
 * @description: 该类用于处理订单信息数据，将输入的JSONObject进行转换和处理，提取所需字段并添加支付时间段信息。
 */
public class MapOrderInfoDataFunc extends RichMapFunction<JSONObject, JSONObject> {
    @Override
    public JSONObject map(JSONObject jsonObject) {
        // 创建一个新的JSONObject用于存储处理后的结果
        JSONObject result = new JSONObject();
        // 检查输入的JSONObject是否包含 "after" 字段
        if (jsonObject.containsKey("after")) {
            // 检查 "after" 字段是否不为空
            if (jsonObject.getJSONObject("after") != null) {
                // 获取 "after" 字段对应的JSONObject
                JSONObject after = jsonObject.getJSONObject("after");
                // 将时间戳毫秒数添加到结果中
                result.put("ts_ms", jsonObject.getLongValue("ts_ms"));
                // 将订单ID添加到结果中
                result.put("id", after.getString("id"));
                // 将用户ID添加到结果中
                result.put("uid", after.getString("user_id"));
                // 将收货人信息添加到结果中
                result.put("consignee", after.getString("consignee"));
                // 将订单创建时间添加到结果中
                result.put("create_time", after.getLongValue("create_time"));
                // 将原始总金额添加到结果中
                result.put("original_total_amount", after.getString("original_total_amount"));
                // 将总金额添加到结果中
                result.put("total_amount", after.getString("total_amount"));
                // 将省份ID添加到结果中
                result.put("province_id", after.getLongValue("province_id"));
                // 获取订单创建时间
                long createTime = after.getLongValue("create_time");
                // 调用 determineLoginPeriod 方法确定支付时间段
                String pay_time_slot = determineLoginPeriod(createTime);
                // 将支付时间段添加到结果中
                result.put("pay_time_slot", pay_time_slot);
            }
        }
        // 返回处理后的结果
        return result;
    }

    /**
     * 根据时间戳确定支付时间段
     * @param timestamp 时间戳（毫秒）
     * @return 支付时间段字符串，如 "凌晨", "早晨" 等
     */
    private String determineLoginPeriod(long timestamp) {
        // 将时间戳转换为 Instant 对象
        Instant instant = Instant.ofEpochMilli(timestamp);
        // 将 Instant 对象转换为本地日期时间对象，使用系统默认时区
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        // 从本地日期时间对象中提取时间部分
        LocalTime time = dateTime.toLocalTime();
        // 获取小时数
        int hour = time.getHour();

        // 根据小时数判断支付时间段
        if (hour < 6) return "凌晨";
        else if (hour < 9) return "早晨";
        else if (hour < 12) return "上午";
        else if (hour < 14) return "中午";
        else if (hour < 18) return "下午";
        else if (hour < 22) return "晚上";
        else return "夜间";
    }
}