package com.reailerv1.func;

import com.stream.util.ConfigUtils;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @Package com.reailerv1.func.HiveCatalogUtils
 * @Author li.yan
 * @Date 2025/5/13 15:40
 * @description:
 */
public class HiveCatalogUtils {
    private static final String HIVE_CONF_DIR = ConfigUtils.getString("hive.conf.dir");

    public static HiveCatalog getHiveCatalog(String catalogName){
        System.setProperty("HADOOP_USER_NAME","root");
        return new HiveCatalog(catalogName, "default", HIVE_CONF_DIR);
    }
}