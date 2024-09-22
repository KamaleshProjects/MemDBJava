package com.reflections.db.norel.model;

public interface DB {

    void createDB();

    void startTxn();

    void endTxn();

    void rollback();

    void deleteDB();
}
