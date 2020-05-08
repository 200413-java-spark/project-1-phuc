package com.github.phuctle.chessdb.startup;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

//creates the spark context
public class CreateContext {
    private JavaSparkContext context;
    private static CreateContext instance;

    //creates a singleton
    private CreateContext(){}
    public static CreateContext getInstance(){
        if (instance == null){
            instance = new CreateContext();
            instance.createContext();
        }
        return instance;
    }

    private void createContext(){
        if (this.context == null){
        SparkConf conf = new SparkConf().setAppName("project-1").setMaster("local");
        this.context = new JavaSparkContext(conf);
            System.out.println("CREATING CONTEXT!!!");
    }
    }
    
    public JavaSparkContext getContext(){
        return this.context;
    }
}