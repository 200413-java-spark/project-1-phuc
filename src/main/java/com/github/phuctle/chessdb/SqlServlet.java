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

        SqlDataSource dataSource = SqlDataSource.getInstance();
        Dao<String[]> sqlDBget = new SqlRepo(dataSource);
        Dao<String[]> sqlDBgetNames = new SqlGetTableNames(dataSource);
        List<String[]> outDataNames = new ArrayList<>();
        List<String[]> outDataTable = new ArrayList<>();
        if (tableSelect != null){
            outDataNames = sqlDBgetNames.readAll(null);
            if (outDataNames != null){
                for(int i =0; i < outDataNames.size(); i++){
                    if (outDataNames.get(i)[0].equals(tableSelect)){

                        StorageVar tableSelectVar = new StorageVar(outDataNames.get(i)[0],outDataNames.get(i)[1],outDataNames.get(i)[2]);

                        outDataTable = sqlDBget.readAll(tableSelectVar);
                        // Read all from database
                        for (int j = 0; j< outDataTable.size();j++) {
                            resp.getWriter().format("%20s", outDataTable.get(j)[0] + " ");
                            resp.getWriter().format("%20s", outDataTable.get(j)[1] + "\n");}
                    }
                }
            }

        }
        else{
            resp.getWriter().println("No table name specified.");
            //Reads all names database
            outDataNames = sqlDBgetNames.readAll(null);
            try{
                if (outDataNames.get(0)[0] != null){
                    resp.getWriter().println("This is a list of table names.\n");
                    for (int i = 0; i< outDataNames.size();i++) {
                        resp.getWriter().println(outDataNames.get(i)[0]);
                    }
                }
            }
            catch(IndexOutOfBoundsException e){
                System.err.println(e);
            }
                
        }
    }

    @Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        System.out.println("POST SQL CALL");  

        String tableSelect = req.getParameter("table");
        
        SqlDataSource dataSource = SqlDataSource.getInstance();

        String sqlString = "DELETE FROM tablenames " +
                           "WHERE tablenames=\'" + tableSelect + "\';";
        String sqlString2 = "DROP TABLE " + tableSelect + ";";

        try(Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();) {
            statement.executeUpdate(sqlString);
            statement.executeUpdate(sqlString2);}
        catch (SQLException e) {
            System.err.println(e.getMessage());}

    }
}