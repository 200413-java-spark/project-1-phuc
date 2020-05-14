package com.github.phuctle.chessdb.io;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SqlRepo implements Dao<String[]> {
    private SqlDataSource dataSource;
    private List<String[]> cache;

    public SqlRepo(SqlDataSource source){
        this.dataSource = source;
        this.cache = new ArrayList<>();
    }

    @Override
    public void insertAll(List<String[]> dataVars) {
        String sql = "insert into singlecol(col1, col2) values(?,?)";
        try(Connection connection = this.dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(sql);){
        for (int i =0; i< dataVars.size();i++){
                statement.setString(1, dataVars.get(i)[0]);
                statement.setString(2, dataVars.get(i)[1]);
                statement.addBatch();
            }
            statement.executeBatch();
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            }

    }

    @Override
    public List<String[]> readAll(String tableSelect) {
        if (cache.isEmpty()){
            String sql = "select * from "+ tableSelect;

            try (Connection connection = this.dataSource.getConnection();
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(sql);){
                
                while(resultSet.next()){
                    String col1s = resultSet.getString("col1");
                    String col2s = resultSet.getString("col2");
                    String[] outResult = new String[2];
                    outResult[0] = col1s;
                    outResult[1] = col2s;
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