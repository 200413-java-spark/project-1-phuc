package com.github.phuctle.chessdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.github.phuctle.chessdb.io.Dao;
import com.github.phuctle.chessdb.io.LoadCSV;
import com.github.phuctle.chessdb.io.SqlDataSource;
import com.github.phuctle.chessdb.io.SqlRepo;
import com.github.phuctle.chessdb.startup.CreateContext;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class SqlServlet extends HttpServlet {
    /**
     *Servlet used to access database
     */
    private static final long serialVersionUID = 2L;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        System.out.println("GET SQL CALL");

                SqlDataSource dataSource = SqlDataSource.getInstance();
                Dao<String[]> sqlDBget = new SqlRepo(dataSource);

            // Read all from database
                    List<String[]> outData = new ArrayList<>();
                    outData = sqlDBget.readAll();
                    for (int i = 0; i< outData.size();i++) {
                        resp.getWriter().format("%20s", outData.get(i)[0] + " ");
                        resp.getWriter().format("%20s", outData.get(i)[1] + "\n");
                    }
    }
    
    @Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        System.out.println("POST SQL CALL");  
    }
}