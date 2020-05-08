package com.github.phuctle.chessdb.fileio;

import com.github.phuctle.chessdb.startup.CreateSparkSession;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LoadCSV {
    public Dataset<Row> getCSVFile(){
        CreateSparkSession session = CreateSparkSession.getInstance();
        SparkSession sparkSession = session.getSession();
        if (sparkSession == null){
            System.out.println("SPARKSESSION CODE FAILURE!!!");
        }
        Dataset<Row> chessDataCSV = sparkSession.read().format("csv").option("header", "true").load("src/resources/games.csv");   

        return chessDataCSV;
    }
    
}