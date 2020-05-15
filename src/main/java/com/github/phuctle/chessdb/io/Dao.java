package com.github.phuctle.chessdb.io;

import java.util.List;

import com.github.phuctle.chessdb.operations.StorageVar;

public interface Dao<E> {

    void insertAll(StorageVar e);

    List<E> readAll(String e);
}