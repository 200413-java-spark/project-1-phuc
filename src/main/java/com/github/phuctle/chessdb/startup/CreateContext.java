package com.github.phuctle.chessdb.startup;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

//creates the spark context
public class CreateContext {
    public JavaSparkContext createContext(){
        SparkConf conf = new SparkConf().setAppName("project-1").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);
        return context;
    }
    
}