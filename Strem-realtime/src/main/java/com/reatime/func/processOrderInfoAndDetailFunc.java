package com.reatime.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Package com.label.func.FilterOrderInfoAndDetailFunc
 * @Author li.yan
 * @Date 2025/5/14 22:51
 * @description: 该类用于过滤订单信息和详情数据，只保留最新的记录。
 */
public class processOrderInfoAndDetailFunc extends KeyedProcessFunction<String, JSONObject, JSONObject> {

    private ValueState<Long> latestTsState;

    @Override
    public void open(Configuration parameters) {

        ValueStateDescriptor<Long> descriptor =
                new ValueStateDescriptor<>("latestTs", Long.class);
        //
        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.hours(1)).build());
        // 获取运行时上下文的状态
        latestTsState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
        // 获取存储的最新时间戳
        Long storedTs = latestTsState.value();
        // 获取当前记录的创建时间
        long currentTs = value.getLong("create_time");

        // 如果存储的时间戳为空或者当前时间戳大于存储的时间戳
        if (storedTs == null || currentTs > storedTs) {
            // 更新存储的最新时间戳
            latestTsState.update(currentTs);
            // 输出当前记录
            out.collect(value);
        }
    }
}