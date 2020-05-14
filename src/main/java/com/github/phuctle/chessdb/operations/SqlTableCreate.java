package com.github.phuctle.chessdb.operations;

public class SqlTableCreate {
    public void makeTable(StorageVar inputVars){
        
        String tableName = "thisIsaTest";
        String testCreate = "CREATE TABLE " + tableName +
                            "(id SERIAL PRIMARY KEY, " +
                            "names VARCHAR, " +
                            "col1names VARCHAR, " +
                            "col2names VARCHAR " +
                            ")";
    }
}