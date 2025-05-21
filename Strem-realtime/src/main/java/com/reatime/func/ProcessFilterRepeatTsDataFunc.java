package com.reatime.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;

/**
 * @Package com.retailersv1.func.ProcessFullDataDeduplication
 * @Author li.yan
 * @Date 2025/5/14 14:55
 * @description: 该类用于对完整数据进行去重处理。
 */
public class ProcessFilterRepeatTsDataFunc extends KeyedProcessFunction<String, JSONObject, JSONObject> {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessFilterRepeatTsDataFunc.class);
    private ValueState<HashSet<String>> processedDataState;

    @Override
    public void open(Configuration parameters) {

        ValueStateDescriptor<HashSet<String>> descriptor = new ValueStateDescriptor<>(
                "processedDataState",
                TypeInformation.of(new org.apache.flink.api.common.typeinfo.TypeHint<HashSet<String>>() {})
        );
        // 获取运行时上下文的状态
        processedDataState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {

        HashSet<String> processedData = processedDataState.value();
        if (processedData == null) {
            // 如果集合为空，则创建一个新的集合
            processedData = new HashSet<>();
        }


        String dataStr = value.toJSONString();
        LOG.info("Processing data: {}", dataStr);
        // 检查集合中是否包含当前记录
        if (!processedData.contains(dataStr)) {
            LOG.info("Adding new data to set: {}", dataStr);
            // 如果不包含，则将当前记录添加到集合中
            processedData.add(dataStr);
            // 更新已处理的数据集合
            processedDataState.update(processedData);
            // 输出当前记录
            out.collect(value);
        } else {
            LOG.info("Duplicate data found: {}", dataStr);
        }
    }
}