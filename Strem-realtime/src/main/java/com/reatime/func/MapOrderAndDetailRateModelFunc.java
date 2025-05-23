package com.reatime.func;

import com.alibaba.fastjson.JSONObject;
import com.reatime.bean.DimBaseCategory;
import com.reatime.bean.DimSkuInfoMsg;
import com.reatime.util.JdbcUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.util.List;

/**
 * @Package com.label.func.MapOrderAndDetailRateModelFunc
 * @Author li.yan
 * @Date 2025/5/15 08:40
 * @description: 该类用于处理订单和订单详情数据，根据不同的维度（时间、价格、品牌、类目）进行打分，并添加到结果中。
 */
public class MapOrderAndDetailRateModelFunc extends RichMapFunction<JSONObject, JSONObject> {

    private Connection connection;
    List<DimSkuInfoMsg> dimSkuInfoMsgs;

    private final List<DimBaseCategory> dimBaseCategories;
    private final double timeRate;
    private final double amountRate;
    private final double brandRate;
    private final double categoryRate;

    /**
     * 构造函数，初始化维度信息和打分权重
     * @param dimBaseCategories 基础类目信息列表
     * @param timeRate 时间维度打分权重
     * @param amountRate 价格维度打分权重
     * @param brandRate 品牌维度打分权重
     * @param categoryRate 类目维度打分权重
     */
    public MapOrderAndDetailRateModelFunc(List<DimBaseCategory> dimBaseCategories, double timeRate, double amountRate, double brandRate, double categoryRate) {
        this.dimBaseCategories = dimBaseCategories;
        this.timeRate = timeRate;
        this.amountRate = amountRate;
        this.brandRate = brandRate;
        this.categoryRate = categoryRate;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 建立 MySQL 数据库连接
        connection = JdbcUtils.getMySQLConnection(
                "jdbc:mysql://cdh03:3306/realtime_v1",  // 替换为你的MySQL URL
                "root",                              // 替换为你的用户名
                "root"                               // 替换为你的密码
        );

        // 定义查询 SKU 信息的 SQL 语句
        String querySkuSql = "select sku_info.id as skuid,      \n" +
                "       spu_info.id as spuid,           \n" +
                "       spu_info.category3_id as c3id,  \n" +
                "       base_trademark.tm_name as tname \n" +
                "from realtime_v1.sku_info              \n" +
                "join realtime_v1.spu_info              \n" +
                "on sku_info.spu_id = spu_info.id       \n" +
                "join realtime_v1.base_trademark        \n" +
                "on realtime_v1.spu_info.tm_id = realtime_v1.base_trademark.id";


        dimSkuInfoMsgs = JdbcUtils.queryList2(connection, querySkuSql, DimSkuInfoMsg.class);
    }

    @Override
    public JSONObject map(JSONObject jsonObject) {
        // 获取 SKU ID
        String skuId = jsonObject.getString("sku_id");
        if (skuId != null && !skuId.isEmpty()) {
            // 遍历 SKU 信息列表，找到匹配的 SKU ID
            for (DimSkuInfoMsg dimSkuInfoMsg : dimSkuInfoMsgs) {
                if (dimSkuInfoMsg.getSkuid().equals(skuId)) {
                    // 将三级类目 ID 添加到结果中
                    jsonObject.put("c3id", dimSkuInfoMsg.getC3id());
                    // 将品牌名称添加到结果中
                    jsonObject.put("tname", dimSkuInfoMsg.getTname());
                    break;
                }
            }
        }

        // 获取三级类目 ID
        String c3id = jsonObject.getString("c3id");
        if (c3id != null && !c3id.isEmpty()) {
            // 遍历基础类目信息列表，找到匹配的三级类目 ID
            for (DimBaseCategory dimBaseCategory : dimBaseCategories) {
                if (c3id.equals(dimBaseCategory.getId())) {
                    // 将一级类目名称添加到结果中
                    jsonObject.put("b1_name", dimBaseCategory.getB1name());
                    break;
                }
            }
        }

        // 时间打分
        String payTimeSlot = jsonObject.getString("pay_time_slot");
        if (payTimeSlot != null && !payTimeSlot.isEmpty()) {
            switch (payTimeSlot) {
                case "凌晨":
                    jsonObject.put("pay_time_18-24", round(0.2 * timeRate));
                    jsonObject.put("pay_time_25-29", round(0.1 * timeRate));
                    jsonObject.put("pay_time_30-34", round(0.1 * timeRate));
                    jsonObject.put("pay_time_35-39", round(0.1 * timeRate));
                    jsonObject.put("pay_time_40-49", round(0.1 * timeRate));
                    jsonObject.put("pay_time_50", round(0.1 * timeRate));
                    break;
                case "早晨":
                    jsonObject.put("pay_time_18-24", round(0.1 * timeRate));
                    jsonObject.put("pay_time_25-29", round(0.1 * timeRate));
                    jsonObject.put("pay_time_30-34", round(0.1 * timeRate));
                    jsonObject.put("pay_time_35-39", round(0.1 * timeRate));
                    jsonObject.put("pay_time_40-49", round(0.2 * timeRate));
                    jsonObject.put("pay_time_50", round(0.3 * timeRate));
                    break;
                // 其他时间段的打分逻辑类似
            }
        }

        // 价格打分
        double totalAmount = jsonObject.getDoubleValue("total_amount");
        if (totalAmount < 1000) {
            jsonObject.put("amount_18-24", round(0.8 * amountRate));
            jsonObject.put("amount_25-29", round(0.6 * amountRate));
            jsonObject.put("amount_30-34", round(0.4 * amountRate));
            jsonObject.put("amount_35-39", round(0.3 * amountRate));
            jsonObject.put("amount_40-49", round(0.2 * amountRate));
            jsonObject.put("amount_50", round(0.1 * amountRate));
        } else if (totalAmount > 1000 && totalAmount < 4000) {
            jsonObject.put("amount_18-24", round(0.2 * amountRate));
            jsonObject.put("amount_25-29", round(0.4 * amountRate));
            jsonObject.put("amount_30-34", round(0.6 * amountRate));
            jsonObject.put("amount_35-39", round(0.7 * amountRate));
            jsonObject.put("amount_40-49", round(0.8 * amountRate));
            jsonObject.put("amount_50", round(0.7 * amountRate));
        } else {
            jsonObject.put("amount_18-24", round(0.1 * amountRate));
            jsonObject.put("amount_25-29", round(0.2 * amountRate));
            jsonObject.put("amount_30-34", round(0.3 * amountRate));
            jsonObject.put("amount_35-39", round(0.4 * amountRate));
            jsonObject.put("amount_40-49", round(0.5 * amountRate));
            jsonObject.put("amount_50", round(0.6 * amountRate));
        }

        // 品牌打分
        String tname = jsonObject.getString("tname");
        if (tname != null && !tname.isEmpty()) {
            switch (tname) {
                case "TCL":
                    jsonObject.put("tname_18-24", round(0.2 * brandRate));
                    jsonObject.put("tname_25-29", round(0.3 * brandRate));
                    jsonObject.put("tname_30-34", round(0.4 * brandRate));
                    jsonObject.put("tname_35-39", round(0.5 * brandRate));
                    jsonObject.put("tname_40-49", round(0.6 * brandRate));
                    jsonObject.put("tname_50", round(0.7 * brandRate));
                    break;
                case "苹果":
                case "联想":
                case "小米":
                    jsonObject.put("tname_18-24", round(0.9 * brandRate));
                    jsonObject.put("tname_25-29", round(0.8 * brandRate));
                    jsonObject.put("tname_30-34", round(0.7 * brandRate));
                    jsonObject.put("tname_35-39", round(0.7 * brandRate));
                    jsonObject.put("tname_40-49", round(0.7 * brandRate));
                    jsonObject.put("tname_50", round(0.5 * brandRate));
                    break;
                // 其他品牌的打分逻辑类似
            }
        }

        // 类目打分
        String b1Name = jsonObject.getString("b1_name");
        if (b1Name != null && !b1Name.isEmpty()) {
            switch (b1Name) {
                case "数码":
                case "手机":
                case "电脑办公":
                case "个护化妆":
                case "服饰内衣":
                    jsonObject.put("b1name_18-24", round(0.9 * categoryRate));
                    jsonObject.put("b1name_25-29", round(0.8 * categoryRate));
                    jsonObject.put("b1name_30-34", round(0.6 * categoryRate));
                    jsonObject.put("b1name_35-39", round(0.4 * categoryRate));
                    jsonObject.put("b1name_40-49", round(0.2 * categoryRate));
                    jsonObject.put("b1name_50", round(0.1 * categoryRate));
                    break;
                case "家居家装":
                case "图书、音像、电子书刊":
                case "厨具":
                case "鞋靴":
                case "母婴":
                case "汽车用品":
                case "珠宝":
                case "家用电器":
                    jsonObject.put("b1name_18-24", round(0.2 * categoryRate));
                    jsonObject.put("b1name_25-29", round(0.4 * categoryRate));
                    jsonObject.put("b1name_30-34", round(0.6 * categoryRate));
                    jsonObject.put("b1name_35-39", round(0.8 * categoryRate));
                    jsonObject.put("b1name_40-49", round(0.9 * categoryRate));
                    jsonObject.put("b1name_50", round(0.7 * categoryRate));
                    break;
                // 其他类目的打分逻辑类似
            }
        }

        // 返回处理后的结果
        return jsonObject;
    }

    @Override
    public void close() throws Exception {
        // 关闭数据库连接
        if (connection != null) {
            connection.close();
        }
    }

    /**
     * 四舍五入保留三位小数
     * @param value 要四舍五入的值
     * @return 四舍五入后的值
     */
    private static double round(double value) {
        return BigDecimal.valueOf(value)
                .setScale(3, RoundingMode.HALF_UP)
                .doubleValue();
    }
}