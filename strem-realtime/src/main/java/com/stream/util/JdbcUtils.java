package com.stream.util;

import com.alibaba.fastjson2.util.BeanUtils;
import org.apache.flink.shaded.curator5.com.google.common.base.CaseFormat;


import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;


/**
 * @Package com.stream.common.utils.JdbcUtils
 * @Author zhou.han
 * @Date 2024/12/20 08:51
 * @description: MySQL Utils
 */
public class JdbcUtils {

    public static Connection getMySQLConnection(String mysqlUrl, String username, String pwd) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        return DriverManager.getConnection(mysqlUrl, username, pwd);
    }


    public static void closeMySQLConnection(Connection conn) throws SQLException {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    public static <T> List<T> queryList(Connection conn, String sql, Class<T> clz, boolean... isUnderlineToCamel) throws Exception {
        List<T> resList = new ArrayList<>();
        boolean defaultIsUToC = false;

        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData metaData = rs.getMetaData();
        while (rs.next()) {
            //通过反射创建一个对象，用于接收查询结果
            T obj = clz.newInstance();

            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                Object columnValue = rs.getObject(i);

                if (defaultIsUToC) {
                    columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                }

                // 使用 PropertyDescriptor 设置属性
                try {
                    PropertyDescriptor pd = new PropertyDescriptor(columnName, clz);
                    Method writeMethod = pd.getWriteMethod();
                    if (writeMethod != null) {
                        writeMethod.invoke(obj, columnValue);
                    }
                } catch (Exception e) {
                    // 属性不存在或类型不匹配时忽略或记录日志
                    // e.printStackTrace();
                }
            }
        }
        return null;
    }
}