package com.github.phuctle.chessdb.io;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.github.phuctle.chessdb.operations.StorageVar;

public class SqlRepo implements Dao<String[]> {
    private SqlDataSource dataSource;
    private List<String[]> cache;

    public SqlRepo(SqlDataSource source){
        this.dataSource = source;
        this.cache = new ArrayList<>();
    }

    @Override
    public void insertAll(StorageVar dataVars) {
        String tName = dataVars.getTableName();
        String c1Name = dataVars.getCol1Name();
        String c2Name = dataVars.getCol2Name();

        String sql = "insert into "+tName+"("+c1Name+", "+c2Name+") values(?,?)";
        try(Connection connection = this.dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(sql);){
        for (int i =0; i< dataVars.getCache().size();i++){
                statement.setString(1, dataVars.getCache().get(i)[0]);
                statement.setString(2, dataVars.getCache().get(i)[1]);
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