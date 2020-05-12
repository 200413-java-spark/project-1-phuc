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

public class SparkServlet extends HttpServlet {
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
        System.out.println("GET TEST");
        String fileName = req.getParameter("file");
        if (fileName != null) {
            resp.getWriter().println("These are the names of the columns: \n");

    
            JavaRDD<String> chessDataCSV = new LoadCSV().getCSVFileContext(fileName);
            JavaRDD<String[]> chessDataColumnsPre = chessDataCSV.map((x) -> x.split(","));
            String[] header = chessDataColumnsPre.take(1).get(0);
    
            for (int i =0; i < header.length; i++){
                resp.getWriter().println(i + ". " +header[i]);;
            }
    
            resp.getWriter().println("\nSelect up to two columns using ?col1=[]&col2=[]&op=[].\n"+
                "Single column operations available are: ave, count\n"+
                "If two columns are selected, the first column is the primary key and\n"+
                "and the operation is performed on the second column.");
        }
        else{
            resp.getWriter().println("This needs a file parameter.");
        }

        //String headerTag = header[0];
        //JavaRDD<String[]> chessDataCol = chessDataColumnsPre.filter((x) -> (!(x[0].equals(headerTag))));

        /*
        for (String[] line:chessDataCol.take(20)){
            resp.getWriter().println("* "+line[0]);}
            */

    }
    
    @Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        System.out.println("POST TEST");
        //Dataset<Row> chessDataCSV = new LoadCSV().getCSVFileSession();
        //chessDataCSV.show();
        String col1s = req.getParameter("col1");
        String col2s = req.getParameter("col2");
        String col3s = req.getParameter("col3");
        String op = req.getParameter("op");
        String fileName = req.getParameter("file");

        try {
            if (col1s != null){
                int col1 = Integer.parseInt(col1s);
            }   
            if (col2s != null){
                int col2 = Integer.parseInt(col2s);
            }  
            if (col3s != null){
                int col3 = Integer.parseInt(col3s);
            }    
        } catch (NumberFormatException e) {
            System.err.println("Column values need to be integers.");
            System.exit(0);
        }

        if (col1s != null && op != null){
            //load in file as an RDD
            JavaRDD<String> chessDataCSV = new LoadCSV().getCSVFileContext(fileName);
            //splits the lines into a string array
            JavaRDD<String[]> chessDataColumnsPre = chessDataCSV.map((x) -> x.split(","));
            //identifies the first row as the header
            String[] header = chessDataColumnsPre.take(1).get(0);
            String headerTag = header[0];
            //removes the header from the RDD
            JavaRDD<String[]> chessDataCol = chessDataColumnsPre
                .filter((x) -> (!(x[0].equals(headerTag))));

            //pulls out everything from selected column into new RDD for column 1
            JavaRDD<String> selectedCol1 = chessDataCol
                .map(x -> x[Integer.parseInt(col1s)]);
        
            if (col2s != null){
                switch (op) {
                    case "ave":
                        JavaPairRDD<String, Integer> selectedColCount = selectedCol1
                            .mapToPair(x -> new Tuple2(x,1))
                            .reduceByKey((a,b) -> ((int)a+(int)b));

                        //isolates the two selected columns
                        JavaPairRDD<String,Integer> selectedPairCol = chessDataCol
                            .mapToPair(x -> new Tuple2(x[Integer.parseInt(col1s)],Integer.parseInt(x[Integer.parseInt(col2s)])));
                        JavaPairRDD<String,Integer> totalValuePair = selectedPairCol
                            .reduceByKey((a,b) -> ((int)a+(int)b));

                        JavaPairRDD<String, Tuple2<Integer, Integer>> joinedRDD = totalValuePair.join(selectedColCount);
                        JavaPairRDD<String, Integer> joinedAveRDD = joinedRDD.mapToPair((x) -> new Tuple2(x._1,(x._2._1 / x._2._2)));
                        resp.getWriter().println(joinedAveRDD.collect());
                        break;
                
                    default:
                        break;
                }
            }
            else{
                switch (op) {
                    case "count":
                        JavaPairRDD<String, Integer> selectedColCount = selectedCol1
                            .mapToPair(x -> new Tuple2(x,1))
                            .reduceByKey((a,b) -> ((int)a+(int)b));
                        resp.getWriter().println(selectedColCount.collect());
                        break;
                    case "ave":
                        JavaRDD<Integer> colInt = selectedCol1
                            .map(x -> Integer.parseInt(x));
                        int average = (int) (colInt.reduce((a, b) -> ((int) a + (int) b)) / colInt.count());
                        resp.getWriter().println("The average of column "+col1s+" is "+average);
                        break;
                    default:
                        break;
                }
            }  
        }
    }
}