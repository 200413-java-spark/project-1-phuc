package com.github.phuctle.chessdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.github.phuctle.chessdb.fileio.LoadCSV;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ServletTest extends HttpServlet {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    //List<String> testList = new ArrayList<>();
    //JavaSparkContext sparkContext;
    //Dataset<Row> chessDataCSV;
/*
    @Override
    public void init() throws ServletException {
        SparkConf conf = new SparkConf()
        .setAppName("ChessTable")
        .setMaster("local");
        sparkContext = new JavaSparkContext(conf);
        // create session to load csv
        SparkSession sparkSession = SparkSession.builder().master("local").appName("Spark Test").getOrCreate();
        // reads in csv
        chessDataCSV = sparkSession.read().format("csv").option("header", "true").load("games.csv");
    }
*/
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.getWriter().println("This is a get test.");
        System.out.println("GET TEST");

        Dataset<Row> chessDataCSV = new LoadCSV().getCSVFile();
        RDD<Row> blackwins1 = chessDataCSV.rdd();
        JavaRDD<Row> blackWinsRDD = blackwins1.toJavaRDD();
        
    }
    
    @Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.getWriter().println("This a post test.");
        System.out.println("POST TEST");
        Dataset<Row> chessDataCSV = new LoadCSV().getCSVFile();
        chessDataCSV.show();
    }
}