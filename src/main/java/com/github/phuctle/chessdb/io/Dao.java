package com.github.phuctle.chessdb.io;

import java.util.List;

public interface Dao<E> {

    void insertAll(List<E> e);

    List<E> readAll();
}