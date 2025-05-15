package com.reatime;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.reatime.util.Config;
import com.reatime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Package PACKAGE_NAME.Log_Disk
 * @Author li.yan
 * @Date 2025/5/13 16:18
 * @description: 处理日志数据
 */
public class Log_Disk {
    public static void main(String[] args) throws Exception {
        // 1. 初始化Flink执行环境
        // 创建一个流执行环境，用于构建和执行Flink作业
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1
        env.setParallelism(1);

        // 2. 获取Kafka数据源
        // 从Kafka主题topic_log中读取日志数据
        DataStreamSource<String> topic_log = KafkaUtil.getKafkaConsumer(env, Config.KAFKA_BOOT_SERVER, "topic_log");

        // 3. 处理日志数据
        // 将JSON字符串转换为JSONObject对象
        SingleOutputStreamOperator<JSONObject> Topic_log_json = topic_log.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) {
                return JSONObject.parseObject(s);
            }
        });

        // 4. 获取基础标签topic的数据
        // 从Kafka主题disk中读取基础标签数据
        DataStreamSource<String> disk = KafkaUtil.getKafkaConsumer(env, Config.KAFKA_BOOT_SERVER, "disk");
        // 将JSON字符串转换为JSONObject对象，并分配时间戳和水印，允许20秒的乱序
        SingleOutputStreamOperator<JSONObject> disk_json = disk.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) {
                return JSON.parseObject(s);
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withTimestampAssigner((event, timestamp) -> event.getLong("ts")));
        // 打印基础标签数据
        disk_json.print();

        // 5. 处理日志数据
        // 从日志数据中提取设备ID、用户ID和时间戳，并封装成新的JSONObject对象
        SingleOutputStreamOperator<JSONObject> process = Topic_log_json.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) {
                JSONObject log_disk = new JSONObject();
                JSONObject common = jsonObject.getJSONObject("common");
                String mid = common.getString("mid");
                String uid = common.getString("uid");
                Long ts = jsonObject.getLong("ts");
                log_disk.put("mid", mid);
                if (uid != null) {
                    log_disk.put("uid", uid);
                }
                log_disk.put("ts", ts);
                collector.collect(log_disk);
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withTimestampAssigner((event, timestamp) -> event.getLong("ts")));
        // 打印处理后的日志数据
        process.print("数据处理");

        // 6. 执行Flink作业
        env.execute("Log Disk Processing");
    }
}