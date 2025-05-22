package com.reatime;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Package PACKAGE_NAME.Damo_Disk
 * @Author li.yan
 * @Date 2025/5/14 22:20
 * @description:
 */
public class Damo_Disk {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setTopics("disk_data")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> disk = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        // 将JSON字符串转换为JSONObject对象
        SingleOutputStreamOperator<JSONObject> disk_json = disk.map((MapFunction<String, JSONObject>) JSON::parseObject);
        // 打印数据源数据
        disk_json.print("数据源数据");

        // 定义侧输出标签定义一个侧输出标签，用于输出用户补充信息数据
        OutputTag<String> user_info_sup_msg = new OutputTag<String>("user_info_sup_msg") {
        };
        // 处理JSON数据，根据表名进行分流
        SingleOutputStreamOperator<String> process = disk_json.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) {
                String table = jsonObject.getJSONObject("source").getString("table");
                if (table != null && table.equals("user_info")) {
                    // 如果表名为user_info，将数据发送到主流
                    collector.collect(jsonObject.toJSONString());
                } else if (table != null && table.equals("user_info_sup")){
                    // 如果表名为user_info_sup，将数据发送到侧输出流
                    context.output(user_info_sup_msg, jsonObject.toString());
                }
            }
        });
        // 获取侧输出流
        SideOutputDataStream<String> userinfo_sup_msg = process.getSideOutput(user_info_sup_msg);
        // 打印主流数据
        process.print("主流数据");
        // 打印侧输出流数据
        userinfo_sup_msg.print("测流数据");

        // 5. 配置Kafka Sink
        // 构建一个Kafka Sink，用于将主流数据写入Kafka主题user_info
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("user_info")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

        process.sinkTo(sink);

        // 6. 配置另一个Kafka Sink
        // 构建一个Kafka Sink，用于将侧输出流数据写入Kafka主题user_info_sup_msg
        KafkaSink<String> sink2 = KafkaSink.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("user_info_sup_msg")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();
        // 将侧输出流数据写入Kafka主题user_info_sup_msg
        userinfo_sup_msg.sinkTo(sink2);

        // 7. 执行作业
        env.execute("Flink Kafka Consumer");
    }
}