package com.github.phuctle.chessdb;

import java.io.File;

import com.github.phuctle.chessdb.startup.CreateContext;
import com.github.phuctle.chessdb.startup.CreateSparkSession;
import com.github.phuctle.chessdb.startup.TomcatRun;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.Wrapper;
import org.apache.catalina.startup.Tomcat;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class ChessDB {
    public static void main(String[] args) throws LifecycleException{

        TomcatRun startupServer = new TomcatRun(); //starts up the Tomcat container
        startupServer.runTomcat();

        CreateContext.getInstance(); //starts Spark
        //CreateSparkSession.getInstance(); 

    }
    
}