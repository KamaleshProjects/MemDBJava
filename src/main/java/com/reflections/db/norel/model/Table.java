package com.reflections.db.norel.model;

import java.util.Map;

public interface Table {

    void createIndex(String columnName);

    void insertRow(Map<String, String> row);

    String[][] readRows(String columnName, String value);

    void updateRow(String columnName, String value, Map<String, String> updateValues);

    void deleteRow(String columnName, String value);

    String getName();

    String[] getColumns();

    int getColCount();

    int getRowCount();
}
