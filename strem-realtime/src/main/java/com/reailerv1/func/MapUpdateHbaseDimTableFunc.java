package com.reailerv1.func;

import com.alibaba.fastjson2.JSONObject;
import com.stream.util.HbaseUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.sql.Connection;

/**
 * @Package com.reailerv1.func.MapUpdateHbaseDimTableFunc
 * @Author li.yan
 * @Date 2025/5/13 16:43
 * @description:
 */
public class MapUpdateHbaseDimTableFunc extends RichMapFunction<JSONObject,JSONObject> {

    private Connection connection;
    private final String hbaseNameSpace;
    private final String zkHostList;
    private HbaseUtils hbaseUtils;

    public MapUpdateHbaseDimTableFunc(String cdhZookeeperServer, String cdhHbaseNameSpace) {
        this.zkHostList = cdhZookeeperServer;
        this.hbaseNameSpace = cdhHbaseNameSpace;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        hbaseUtils = new HbaseUtils(zkHostList);
        connection = hbaseUtils.getConnection();
    }

    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        String op = jsonObject.getString("op");
        if ("d".equals(op)){
            hbaseUtils.deleteTable(jsonObject.getJSONObject("before").getString("sink_table"));
        }else if ("r".equals(op) || "c".equals(op)){
            // String[] columnName = jsonObject.getJSONObject("after").getString("sink_columns").split(",");
            String tableName = jsonObject.getJSONObject("after").getString("sink_table");
            if (!hbaseUtils.tableIsExists(hbaseNameSpace+":"+tableName)){
                hbaseUtils.createTable(hbaseNameSpace,tableName);
            }
        }else {
            hbaseUtils.deleteTable(jsonObject.getJSONObject("before").getString("sink_table"));
            // String[] columnName = jsonObject.getJSONObject("after").getString("sink_columns").split(",");
            String tableName = jsonObject.getJSONObject("after").getString("sink_table");
            hbaseUtils.createTable(hbaseNameSpace,tableName);
        }
        return jsonObject;
    }

    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }
}
