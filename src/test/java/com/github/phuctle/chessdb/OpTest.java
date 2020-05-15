package com.github.phuctle.chessdb;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.github.phuctle.chessdb.startup.CreateContext;
import com.github.phuctle.chessdb.startup.TomcatRun;

import org.apache.catalina.LifecycleException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

public class OpTest {

    @Before
    public void startup() throws LifecycleException {
        TomcatRun startupServer = new TomcatRun(); //starts up the Tomcat container
        startupServer.runTomcat();

        CreateContext.getInstance(); //starts Spark
    }
    
    @Test
    public void singleColTests(){
        CreateContext sc = CreateContext.getInstance();
        //count
        List<String> testNames = new ArrayList<>(Arrays.asList("black","black","black"));
        JavaRDD inputToTest = sc.getContext().parallelize(testNames);
        JavaPairRDD<String, Integer> testRDD = inputToTest.mapToPair(x -> new Tuple2(x, 1))
        .reduceByKey((a, b) -> ((int) a + (int) b)).cache();
        List<Tuple2> control = new ArrayList<>();
        control.add(new Tuple2("black",3));
        assertEquals(control, testRDD.collect());
        //String list = testRDD.collect().toArray().toString();
        //ave

        //top
    }

    @Test
    public void doubleColTests(){
        //ave

        //topave
    }
}