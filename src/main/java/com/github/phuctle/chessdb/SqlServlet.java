package com.github.phuctle.chessdb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.github.phuctle.chessdb.io.Dao;
import com.github.phuctle.chessdb.io.SqlDataSource;
import com.github.phuctle.chessdb.io.SqlRepo;

public class SqlServlet extends HttpServlet {
    /**
     *Servlet used to access database
     */
    private static final long serialVersionUID = 2L;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        System.out.println("GET SQL CALL");

        String tableSelect = req.getParameter("table");

        if (tableSelect != null){
            SqlDataSource dataSource = SqlDataSource.getInstance();
            Dao<String[]> sqlDBget = new SqlRepo(dataSource);

        // Read all from database
                List<String[]> outData = new ArrayList<>();
                outData = sqlDBget.readAll(tableSelect);
                for (int i = 0; i< outData.size();i++) {
                    resp.getWriter().format("%20s", outData.get(i)[0] + " ");
                    resp.getWriter().format("%20s", outData.get(i)[1] + "\n");
                }
        }
        else{
            resp.getWriter().println("No table name specified");
        }

    }
    
    @Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        System.out.println("POST SQL CALL");  

        SqlDataSource dataSource = SqlDataSource.getInstance();

        String tableName = "thisIsaTest";
        String testCreate = "CREATE TABLE " + tableName +
                            "(id SERIAL PRIMARY KEY, " +
                            "names VARCHAR, " +
                            "col1names VARCHAR, " +
                            "col2names VARCHAR " +
                            ")";

        try(Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();) {
                statement.executeUpdate(testCreate);
            }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            }
    }

}