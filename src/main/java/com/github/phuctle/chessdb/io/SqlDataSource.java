package com.github.phuctle.chessdb.io;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SqlDataSource {

    private static SqlDataSource instance;
    private String ip;
    private String url;
    private String user;
    private String password;

    private SqlDataSource() {
        //URL needs to be changed accordingly when the IP for the ec2 changes
        ip = "18.221.35.79";
        url = System.getProperty("database.url", "jdbc:postgresql://"+ip+":5432/chessdb");
        user = System.getProperty("database.username", "chessdb");
        password = System.getProperty("database.password", "chessdb");
    }

    public static SqlDataSource getInstance() {
        if (instance == null) {
            instance = new SqlDataSource();
        }
        return instance;
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }
    
}