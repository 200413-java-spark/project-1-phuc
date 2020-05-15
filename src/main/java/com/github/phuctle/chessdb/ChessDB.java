package com.github.phuctle.chessdb;

import com.github.phuctle.chessdb.startup.CreateContext;
import com.github.phuctle.chessdb.startup.TomcatRun;

import org.apache.catalina.LifecycleException;


public class ChessDB {
    public static void main(String[] args) throws LifecycleException{

        TomcatRun startupServer = new TomcatRun(); //starts up the Tomcat container
        startupServer.runTomcat();

        CreateContext.getInstance(); //starts Spark
    }
    
}