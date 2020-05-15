package com.github.phuctle.chessdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.github.phuctle.chessdb.io.Dao;
import com.github.phuctle.chessdb.io.LoadCSV;
import com.github.phuctle.chessdb.io.SqlDataSource;
import com.github.phuctle.chessdb.io.SqlGetTableNames;
import com.github.phuctle.chessdb.io.SqlRepo;
import com.github.phuctle.chessdb.operations.SqlTableCreate;
import com.github.phuctle.chessdb.operations.StorageVar;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class SparkServlet extends HttpServlet {
    /**
     * Servlet used to interact with Spark
     */
    private static final long serialVersionUID = 1L;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        System.out.println("GET METHOD CALL");
        String fileName = req.getParameter("file");
        if (fileName != null) {
            resp.getWriter().println("These are the names of the columns: \n");

            JavaRDD<String> chessDataCSV = new LoadCSV().getCSVFileContext(fileName);
            JavaRDD<String[]> chessDataColumnsPre = chessDataCSV.map((x) -> x.split(","));
            String[] header = chessDataColumnsPre.take(1).get(0);

            for (int i = 0; i < header.length; i++) {
                resp.getWriter().println(i + ". " + header[i]);
                ;
            }

            resp.getWriter()
                    .println("\nSelect up to two columns using ?col1=[]&col2=[]&op=[].\n"
                            + "Single column operations available are: ave, count, topave, top\n"
                            + "If two columns are selected, the first column is the primary key and\n"
                            + "and the operation is performed on the second column.");
        } else {
            resp.getWriter().println("This needs a file parameter.");
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        System.out.println("POST METHOD CALL");

        String col1s = req.getParameter("col1");
        String col2s = req.getParameter("col2");
        String op = req.getParameter("op");
        String fileName = req.getParameter("file");
        String save = req.getParameter("save");

        Set<String> tabNames = new HashSet<>();
        int flag = 1;

        // checks to make sure that the col values are integers
        try {
            if (col1s != null) {
                int col1 = Integer.parseInt(col1s);
            }
            if (col2s != null) {
                int col2 = Integer.parseInt(col2s);
            }
        } catch (NumberFormatException e) {
            System.err.println("Column values need to be integers.");
            flag = 0;
        }
        
        if (col1s != null && op != null && flag == 1) {
            // load in file as an RDD
            JavaRDD<String> chessDataCSV = new LoadCSV().getCSVFileContext(fileName);
            // splits the lines into a string array
            JavaRDD<String[]> chessDataColumnsPre = chessDataCSV.map((x) -> x.split(","));
            // identifies the first row as the header
            String[] header = chessDataColumnsPre.take(1).get(0);
            if (Integer.parseInt(col1s) < 0 || Integer.parseInt(col1s) > header.length) {
                System.err.println("The value for column 1 is out of bounds.");   
            }

            else{//only runs this code if applicable
            String headerTag = header[0];
            // removes the header from the RDD
            JavaRDD<String[]> chessDataCol = chessDataColumnsPre.filter((x) -> (!(x[0].equals(headerTag))));

            // pulls out everything from selected column into new RDD for column 1
            JavaRDD<String> selectedCol1 = chessDataCol.cache().map(x -> x[Integer.parseInt(col1s)]);
            // counts the number
            JavaPairRDD<String, Integer> selectedColCount = selectedCol1.mapToPair(x -> new Tuple2(x, 1))
                    .reduceByKey((a, b) -> ((int) a + (int) b)).cache();

            if (col2s != null) {
                if (Integer.parseInt(col2s) < 0 || Integer.parseInt(col2s) > header.length) {
                    System.err.println("The value for column 2 is out of bounds.");   
                }
    
                else{
                switch (op) {
                    case "ave":
                        // isolates the two selected columns
                        JavaPairRDD<String, Integer> selectedPairCol = chessDataCol
                                .mapToPair(x -> new Tuple2(x[Integer.parseInt(col1s)],
                                        Integer.parseInt(x[Integer.parseInt(col2s)])));
                        JavaPairRDD<String, Integer> totalValuePair = selectedPairCol
                                .reduceByKey((a, b) -> ((int) a + (int) b)); //obtains the total amount of col 2 by key

                        JavaPairRDD<String, Tuple2<Integer, Integer>> joinedRDD = totalValuePair.join(selectedColCount);
                        JavaPairRDD<String, Integer> joinedAveRDD = joinedRDD
                                .mapToPair((x) -> new Tuple2(x._1, (x._2._1 / x._2._2)));

                        if (save != null && save.equals("yes")) {
                        List<Tuple2<String, Integer>> twoColAve= joinedAveRDD.collect();
                        ArrayList<String[]> pushArray = new ArrayList<>();

                        for (int i = 0; i< twoColAve.size(); i++){
                            String[] topAveKV = {twoColAve.get(i)._1,Integer.toString(twoColAve.get(i)._2)};
                            pushArray.add(topAveKV);}

                        SqlDataSource dataSource = SqlDataSource.getInstance();
                        Dao<String[]> fileInRepo = new SqlRepo(dataSource);

                        Dao<String[]> sqlDBgetNames = new SqlGetTableNames(dataSource);
                        List<String[]> outDataNames = sqlDBgetNames.readAll(null);
                        if (outDataNames != null){
                            for (int i = 0; i< outDataNames.size();i++) {
                                tabNames.add(outDataNames.get(i)[0]);
                            }
                        StorageVar sqlData = new StorageVar(fileName, header[Integer.parseInt(col1s)], header[Integer.parseInt(col2s)], op, pushArray);
                            if (!tabNames.contains(sqlData.getTableName())){
                                SqlTableCreate tableNew = new SqlTableCreate();
                                tableNew.makeTable(sqlData, dataSource);
                                sqlDBgetNames.insertAll(sqlData);
                                fileInRepo.insertAll(sqlData);
                            }
                        }
                    }
                        else{
                            resp.getWriter().println(joinedAveRDD.collect());
                        }
                        break;

                    case "topave":
                        // isolates the two selected columns
                        JavaPairRDD<String, Integer> selectedPairTop = chessDataCol
                                .mapToPair(x -> new Tuple2(x[Integer.parseInt(col1s)],
                                        Integer.parseInt(x[Integer.parseInt(col2s)])));
                        JavaPairRDD<String, Integer> totalValuePairTop = selectedPairTop
                                .reduceByKey((a, b) -> ((int) a + (int) b)); //obtains the total amount of col 2 by key  
                                
                        JavaPairRDD<String, Tuple2<Integer, Integer>> joinedRDDTop = totalValuePairTop.join(selectedColCount);
                        List<Tuple2<String, Integer>> joinedAveListTop = joinedRDDTop
                                .mapToPair(x -> new Tuple2(x._1, (x._2._1 / x._2._2)))
                                .mapToPair(x -> ((Tuple2) x).swap())
                                .sortByKey(false)
                                .mapToPair(x -> ((Tuple2) x).swap())
                                .take(20);
                        if (save != null && save.equals("yes")) {

                        ArrayList<String[]> pushArray2 = new ArrayList<>();
                        for (int i = 0; i< joinedAveListTop.size(); i++){
                            String[] topAveKV = {joinedAveListTop.get(i)._1,Integer.toString(joinedAveListTop.get(i)._2)};
                            pushArray2.add(topAveKV);
                        }
                        SqlDataSource dataSource = SqlDataSource.getInstance();
                        Dao<String[]> fileInRepo = new SqlRepo(dataSource);

                        Dao<String[]> sqlDBgetNames = new SqlGetTableNames(dataSource);
                        List<String[]> outDataNames = sqlDBgetNames.readAll(null);
                        if (outDataNames != null){
                            for (int i = 0; i< outDataNames.size();i++) {
                                tabNames.add(outDataNames.get(i)[0]);
                            }
                        StorageVar sqlData = new StorageVar(fileName, header[Integer.parseInt(col1s)], header[Integer.parseInt(col2s)], op, pushArray2);
                            if (!tabNames.contains(sqlData.getTableName())){
                                SqlTableCreate tableNew = new SqlTableCreate();
                                tableNew.makeTable(sqlData, dataSource);
                                sqlDBgetNames.insertAll(sqlData);
                                fileInRepo.insertAll(sqlData);
                            }
                        }
                        }
                        else{
                            for (int i =0; i< joinedAveListTop.size(); i ++){
                                resp.getWriter().format("%40s",joinedAveListTop.get(i)._1 + " ");
                                resp.getWriter().format("%20s",Integer.toString(joinedAveListTop.get(i)._2) + "\n");
                        }
                    }
                        break;
                    default:
                        break;
                } } }
                else {
                switch (op) {
                    case "count":
                        
                        List<Tuple2<String, Integer>> singleCounter = selectedColCount.collect();
                        // saves to database
                        if (save != null && save.equals("yes")) {
                            
                            ArrayList<String[]> pushArray = new ArrayList<>();
                            for (int i = 0; i< singleCounter.size(); i++){
                                String[] col1Count = {singleCounter.get(i)._1, Integer.toString(singleCounter.get(i)._2)};
                                pushArray.add(col1Count);
                            }
                                    SqlDataSource dataSource = SqlDataSource.getInstance();
                                    Dao<String[]> fileInRepo = new SqlRepo(dataSource);
                                    Dao<String[]> sqlDBgetNames = new SqlGetTableNames(dataSource);
                                    List<String[]> outDataNames = sqlDBgetNames.readAll(null);

                                    if (outDataNames != null){
                                        for (int j = 0; j< outDataNames.size();j++) {
                                            tabNames.add(outDataNames.get(j)[0]);
                                        }
                                    StorageVar sqlData = new StorageVar(fileName, header[Integer.parseInt(col1s)], op, pushArray);
                                        if (!tabNames.contains(sqlData.getTableName())){
                                            SqlTableCreate tableNew = new SqlTableCreate();
                                            tableNew.makeTable(sqlData, dataSource);
                                            sqlDBgetNames.insertAll(sqlData);
                                            fileInRepo.insertAll(sqlData);
                                        }
                                    }

                                }
                            
                        else {
                            resp.getWriter().println(selectedColCount.collect());
                        }
                        break;
                    
                    case "ave":
                        JavaRDD<Integer> colInt = selectedCol1.map(x -> Integer.parseInt(x));
                        int average = (int) (colInt.reduce((a, b) -> ((int) a + (int) b)) / colInt.count());

                        if (save != null && save.equals("yes")) {
                            ArrayList<String[]> pushArray = new ArrayList<>();
                            
                                String[] avePush = {"Ave",Integer.toString(average)};
                                pushArray.add(avePush);
                            
                                SqlDataSource dataSource = SqlDataSource.getInstance();
                                Dao<String[]> fileInRepo = new SqlRepo(dataSource);
                                Dao<String[]> sqlDBgetNames = new SqlGetTableNames(dataSource);
                                List<String[]> outDataNames = sqlDBgetNames.readAll(null);

                                if (outDataNames != null){
                                    for (int j = 0; j< outDataNames.size();j++) {
                                        tabNames.add(outDataNames.get(j)[0]);
                                    }
                                StorageVar sqlData = new StorageVar(fileName, header[Integer.parseInt(col1s)], op, pushArray);
                                    if (!tabNames.contains(sqlData.getTableName())){
                                        SqlTableCreate tableNew = new SqlTableCreate();
                                        tableNew.makeTable(sqlData, dataSource);
                                        sqlDBgetNames.insertAll(sqlData);
                                        fileInRepo.insertAll(sqlData);
                                    }
                                }
                        }
                        else{
                            resp.getWriter().println("The average of column "+col1s+" is "+average);
                        }
                        break;

                    case "top":

                        List<Tuple2<String, Integer>> sortedCol = selectedColCount
                                .mapToPair(x -> x.swap()) //swaps key and value
                                .sortByKey(false)         //sorts by key in descending order so highest value
                                .mapToPair(x -> x.swap()) //is on top
                                .take(20);                //takes the top 20 values to improve performance

                        if (save != null && save.equals("yes")){
                            ArrayList<String[]> pushArray = new ArrayList<>();
                            for (int i = 0; i< sortedCol.size(); i++){
                                String[] topAveKV = {sortedCol.get(i)._1,Integer.toString(sortedCol.get(i)._2)};
                                pushArray.add(topAveKV);
                            }
                            SqlDataSource dataSource = SqlDataSource.getInstance();
                            Dao<String[]> fileInRepo = new SqlRepo(dataSource);
                            Dao<String[]> sqlDBgetNames = new SqlGetTableNames(dataSource);
                            List<String[]> outDataNames = sqlDBgetNames.readAll(null);

                            if (outDataNames != null){
                                for (int j = 0; j< outDataNames.size();j++) {
                                    tabNames.add(outDataNames.get(j)[0]);
                                }
                            StorageVar sqlData = new StorageVar(fileName, header[Integer.parseInt(col1s)], op, pushArray);
                                if (!tabNames.contains(sqlData.getTableName())){
                                    SqlTableCreate tableNew = new SqlTableCreate();
                                    tableNew.makeTable(sqlData, dataSource);
                                    sqlDBgetNames.insertAll(sqlData);
                                    fileInRepo.insertAll(sqlData);
                                }
                            }
                        }
                        else{
                            for (int i =0; i< sortedCol.size(); i ++){
                                resp.getWriter().format("%40s",sortedCol.get(i)._1 + " ");
                                resp.getWriter().format("%20s",Integer.toString(sortedCol.get(i)._2) + "\n");
                        }
                    }
                    default:
                        break;
                }
            }
        }
    }
}
} 
