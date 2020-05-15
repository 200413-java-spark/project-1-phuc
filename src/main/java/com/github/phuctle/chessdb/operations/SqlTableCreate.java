package com.github.phuctle.chessdb.operations;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.github.phuctle.chessdb.io.SqlDataSource;

public class SqlTableCreate {

    public SqlTableCreate(){}

    public void makeTable(StorageVar inputVars, SqlDataSource dataSource){
        
        //SqlDataSource dataSource = SqlDataSource.getInstance();

        String tableName = inputVars.getTableName();
        String col1Name = inputVars.getCol1Name();
        String col2Name = inputVars.getCol2Name();

        String sqlString = "CREATE TABLE " + tableName +
                            "(id SERIAL PRIMARY KEY, " +
                            col1Name + " VARCHAR, " +
                            col2Name +" VARCHAR " +
                            ")";

        try(Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();) {
                statement.executeUpdate(sqlString);
            }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            }
        //return sqlString;
    }
}