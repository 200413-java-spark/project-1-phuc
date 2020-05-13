package com.github.phuctle.chessdb.startup;

import java.io.File;

import com.github.phuctle.chessdb.SparkServlet;
import com.github.phuctle.chessdb.SqlServlet;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;

public class TomcatRun {

    public void runTomcat() throws LifecycleException {
        Tomcat tomcat = new Tomcat();
        tomcat.setBaseDir(new File("target/tomcat/").getAbsolutePath());
        tomcat.setPort(8080);
        tomcat.getConnector();
        tomcat.addWebapp("/chessdb",new File("src/main/webapp/").getAbsolutePath());
        tomcat.addServlet("/chessdb","SparkServlet",new SparkServlet())
            .addMapping("/spark");
        tomcat.addServlet("/chessdb","SqlServlet",new SqlServlet())
            .addMapping("/sql");
        tomcat.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run(){
                try {
                    System.out.println("Shutting down tomcat.");
                    tomcat.stop();
                } catch (LifecycleException e) {
                    e.printStackTrace();
                }
            }
        });
    }
    
}