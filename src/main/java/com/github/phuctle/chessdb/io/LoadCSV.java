package com.github.phuctle.chessdb.io;

import com.github.phuctle.chessdb.startup.CreateContext;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class LoadCSV {
    //CreateSparkSession session = CreateSparkSession.getInstance();
    CreateContext context = CreateContext.getInstance();
    /*
    public Dataset<Row> getCSVFileSession(String fileName){
        
        SparkSession sparkSession = this.session.getSession();
        if (sparkSession == null){
            System.out.println("SPARKSESSION CODE FAILURE!!!");
        }
        Dataset<Row> chessDataCSV = sparkSession.read().format("csv")
            .option("header", "true").load("src/resources/"+fileName);   

        return chessDataCSV;
    }*/
    
    public JavaRDD<String> getCSVFileContext(String fileName){
        
        JavaSparkContext sc = context.getContext();
        if (sc == null){
            System.out.println("SPARKCONTEXT CODE FAILURE!!!");
        }
        JavaRDD<String> chessStringRDD = sc.textFile("src/resources/"+fileName);

        return chessStringRDD;
    }
}