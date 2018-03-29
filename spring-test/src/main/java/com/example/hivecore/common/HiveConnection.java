package com.example.hivecore.common;
import java.sql.Connection;
import java.sql.DriverManager;

public class HiveConnection {

    private static final String URLHIVE = "jdbc:hive2://192.168.214.133:10000/default";//hive服务地址
    private static Connection connection = null;

    public static Connection getHiveConnection() {
        if (null == connection) {
            synchronized (HiveConnection.class) {
                if (null == connection) {
                    try {
                        Class.forName("org.apache.hive.jdbc.HiveDriver");
                        connection = DriverManager.getConnection(URLHIVE, "root", "root");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return connection;
    }
}
