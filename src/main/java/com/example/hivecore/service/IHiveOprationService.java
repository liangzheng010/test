package com.example.hivecore.service;

import java.sql.Connection;
import java.sql.ResultSet;

public interface IHiveOprationService {

    boolean CreateTable(Connection connection);

    boolean CreateTableByTableName(Connection connection, String tableName);

    int Insert(Connection connection);

    ResultSet SearchTable(Connection connection);

    int CountTable(Connection connection, String tableName);
}
