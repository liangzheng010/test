package com.example.hivecore.service.impl;

import com.example.hivecore.dao.IHiveOprationDao;
import com.example.hivecore.service.IHiveOprationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


import java.sql.Connection;
import java.sql.ResultSet;

@Service
public class HiveOprationServiceImpl implements IHiveOprationService {

    @Autowired
    IHiveOprationDao hiveOprationDao;


    @Override
    public boolean CreateTable(Connection connection) {

        hiveOprationDao.CreateTable(connection);
        return true;
    }

    @Override
    public boolean CreateTableByTableName(Connection connection, String tableName) {
        return hiveOprationDao.CreateTableByTableName(connection, tableName);
    }

    @Override
    public int Insert(Connection connection) {

        int result = hiveOprationDao.Insert(connection);
        return 0;
    }

    @Override
    public ResultSet SearchTable(Connection connection) {
        return hiveOprationDao.SearchTable(connection);
    }

    @Override
    public int CountTable(Connection connection, String tableName) {
        return hiveOprationDao.CountTable(connection, tableName);
    }
}
