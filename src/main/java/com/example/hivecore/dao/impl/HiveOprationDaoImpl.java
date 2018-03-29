package com.example.hivecore.dao.impl;

import com.example.hivecore.dao.IHiveOprationDao;
import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

@Repository
public class HiveOprationDaoImpl implements IHiveOprationDao {

    @Override
    public boolean CreateTable(Connection connection) {
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            String createSQL = "create table java_test2 (id string, name string) row format delimited fields terminated by '\\t' ";
            stmt.execute(createSQL);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            close(stmt);
        }
        return true;
    }

    @Override
    public boolean CreateTableByTableName(Connection connection, String tableName) {
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            String createSQL = "create table "  + tableName + "  (id string, name string) row format delimited fields terminated by '\\t' ";
            stmt.execute(createSQL);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            close(stmt);
        }
        return true;
    }

    @Override
    public int Insert(Connection connection) {
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            String filepath = "D://1.txt";
            String insertSQL = "load data local inpath '" + filepath + "' into table java_test2 ";
            stmt.execute(insertSQL);
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        } finally {
            close(stmt);
        }
        return 1;
    }

    @Override
    public ResultSet SearchTable(Connection connection) {
        ResultSet  result = null;
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            String searchSQL = "select * from java_test2  where name is not null ";
            result = stmt.executeQuery(searchSQL);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            close(stmt);
        }
        return result;
    }

    @Override
    public int CountTable(Connection connection, String tableName) {
        ResultSet  result = null;
        Statement stmt = null;
        int count = 0;
        try {
            stmt = connection.createStatement();
            String searchSQL = "select count(1)  from "+ tableName ;
            result = stmt.executeQuery(searchSQL);
            while (result.next()) {
                count = result.getInt(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return count;
        } finally {
            close(stmt);
        }
        return count;
    }

    private void close(Statement stmt){
        try {
            if (stmt != null) {
                stmt.close();
                stmt = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
