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
import com.github.phuctle.chessdb.io.SqlGetTableNames;
import com.github.phuctle.chessdb.io.SqlRepo;
import com.github.phuctle.chessdb.operations.SqlTableCreate;
import com.github.phuctle.chessdb.operations.StorageVar;

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
            resp.getWriter().println("No table name specified.");

            SqlDataSource dataSource = SqlDataSource.getInstance();
            Dao<String[]> sqlDBgetNames = new SqlGetTableNames(dataSource);

            //Reads all names database
            List<String[]> outData = new ArrayList<>();
            outData = sqlDBgetNames.readAll(null);
                if (outData != null){
                    for (int i = 0; i< outData.size();i++) {
                        resp.getWriter().format("%20s", outData.get(i)[0] + " ");
                        resp.getWriter().format("%20s", outData.get(i)[1] + " ");
                        resp.getWriter().format("%20s", outData.get(i)[2] + "\n");
                    }
                }
        }
    }
    
    @Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        System.out.println("POST SQL CALL");  

        SqlDataSource dataSource = SqlDataSource.getInstance();
            Dao<String[]> sqlDBgetNames = new SqlGetTableNames(dataSource);

        // Read all from database
                List<String[]> outData = new ArrayList<>();
                outData = sqlDBgetNames.readAll(null);
                if (outData != null){
                    for (int i = 0; i< outData.size();i++) {
                        resp.getWriter().format("%20s", outData.get(i)[0] + " ");
                        resp.getWriter().format("%20s", outData.get(i)[1] + " ");
                        resp.getWriter().format("%20s", outData.get(i)[2] + "\n");
                    }
                }
        ArrayList<String[]> uselessTestDummy = new ArrayList<>();

        StorageVar test1 = new StorageVar("table1.csv", "header1" , "op1", uselessTestDummy);
        StorageVar test2 = new StorageVar("table2.csv", "header1dash2", "header2dash2", "op2", uselessTestDummy);
        

        SqlTableCreate newTables = new SqlTableCreate();
            newTables.makeTable(test1,dataSource);
            sqlDBgetNames.insertAll(test1);
            newTables.makeTable(test2, dataSource);
            sqlDBgetNames.insertAll(test2);

    }
}