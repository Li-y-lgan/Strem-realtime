package com.reatime;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @Package PACKAGE_NAME.FlinkCDC
 * @Author li.yan
 * @Date 2025/5/13 16:44
 * @description: 获取mysql表数据
 */
public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        // 1. 配置Debezium属性
        // Debezium是一个用于捕获数据库变更的工具，这里配置了一些处理小数的属性
        Properties prop = new Properties();
        prop.put("decimal.handling.mode", "string");
        prop.put("connect.decimal.precision", "16");
        prop.put("connect.decimal.scale", "2");

        // 2. 准备Flink执行环境
        // 创建一个流执行环境，用于构建和执行Flink作业
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1，即所有操作都在一个线程中执行
        env.setParallelism(1);
        // 启用检查点机制，每3000毫秒进行一次检查点操作，用于故障恢复
        env.enableCheckpointing(3000);

        // 3. 使用Flink CDC读取MySQL表中的数据
        // 构建一个MySQL数据源，指定MySQL服务器的地址、端口、数据库名、表名、用户名、密码等信息
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("cdh03")
                .port(3306)
                .databaseList("realtime_v1") // 设置要捕获的数据库
                .tableList("realtime_v1.*") // 设置要捕获的表
                .username("root")
                .password("root")
                // 使用JsonDebeziumDeserializationSchema将源记录转换为JSON字符串
                .deserializer(new JsonDebeziumDeserializationSchema())
                // 从最早的可用位置开始捕获变更
                .startupOptions(StartupOptions.earliest())
                // 包含架构变更
                .includeSchemaChanges(true)
                // 设置Debezium属性
                .debeziumProperties(prop)
                .build();

        // 4. 配置Kafka Sink
        // 构建一个Kafka Sink，用于将数据写入Kafka主题
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("disk_data")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

        // 5. 创建数据流并进行处理
        // 从MySQL数据源创建数据流，不生成水印
        DataStreamSource<String> mySQL_source = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
        // 将数据流写入Kafka主题
        mySQL_source.sinkTo(sink);
        // 打印数据流到控制台
        mySQL_source.print();

        // 6. 执行Flink作业
        env.execute("Print MySQL Snapshot + Binlog");
    }
}