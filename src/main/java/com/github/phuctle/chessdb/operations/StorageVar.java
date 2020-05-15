package com.github.phuctle.chessdb.operations;

import java.util.ArrayList;
import java.util.regex.Pattern;

public class StorageVar {
    private String nameOfFile;
    private String headerName1;
    private String headerName2;
    private String operationName;
    private ArrayList<String[]> cacheVals;
    private String tableName;
    private String col1Name;
    private String col2Name;

    public StorageVar(String nameF, String hName1, String oName, ArrayList<String[]> cVals){
        this.nameOfFile =  nameF.split(Pattern.quote("."))[0];
        this.headerName1 = hName1;
        this.operationName = oName;
        this.cacheVals = cVals;

        
        createSqlStmt();
    }

    public StorageVar(String nameF, String hName1, String hName2, String oName, ArrayList<String[]> cVals){
        this.nameOfFile = nameF.split(Pattern.quote("."))[0];
        this.headerName1 = hName1;
        this.headerName2 = hName2;
        this.operationName = oName;
        this.cacheVals = cVals;

        createSqlStmt();
    }

    private void createSqlStmt(){
        if (headerName2 != null){
            this.tableName = this.nameOfFile
                            +this.headerName1
                            +this.operationName
                            +"on"
                            +this.headerName2;

            this.col1Name = this.headerName1;
            this.col2Name = this.operationName 
                            +"on"
                            +this.headerName2;

        }
        else{
            this.tableName = this.nameOfFile 
                            +this.headerName1 
                            +this.operationName;

            this.col1Name = this.headerName1;
            this.col2Name = this.operationName;
        }
    }

    public String getTableName(){
        return this.tableName;
    }

    public String getCol1Name(){
        return this.col1Name;
    }

    public String getCol2Name(){
        return this.col2Name;
    }

    public ArrayList<String[]> getCache(){
        return this.cacheVals;
    }
   
}