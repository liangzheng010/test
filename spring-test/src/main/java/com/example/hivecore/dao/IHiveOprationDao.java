package com.example.hivecore.dao;

import java.sql.Connection;
import java.sql.ResultSet;

public interface IHiveOprationDao {

    boolean CreateTable(Connection connection);

    boolean CreateTableByTableName(Connection connection, String tableName);

    int Insert(Connection connection);

    ResultSet SearchTable(Connection connection);

    int CountTable(Connection connection, String tableName);
}
