package com.github.phuctle.chessdb.io;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SqlGetTableNames implements Dao<String[]> {
    private SqlDataSource dataSource;
    private List<String[]> cache;

    public SqlGetTableNames(SqlDataSource source){
        this.dataSource = source;
        this.cache = new ArrayList<>();
    }


    @Override
    public void insertAll(List<String[]> e) {
        // TODO Auto-generated method stub

    }

    @Override
    public List<String[]> readAll(String tableNames) {
        if (cache.isEmpty()){
            String sql = "select * from tablenames";

            try (Connection connection = this.dataSource.getConnection();
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(sql);){
                
                while(resultSet.next()){
                    String tNames = resultSet.getString("tablenames");
                    String col1Names = resultSet.getString("col1names");
                    String col2Names = resultSet.getString("col2names");
                    String[] outResult = new String[3];
                    outResult[0] = tNames;
                    outResult[1] = col1Names;
                    outResult[3] = col2Names;
                    cache.add(outResult);
                }
            } catch(SQLException e){
                System.err.println(e.getMessage());
                }   
            return cache;} 
        else {
            return cache;}
    }   
    }