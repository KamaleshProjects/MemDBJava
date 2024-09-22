package com.reflections.db.norel.service;

import java.util.Map;

public class ValidationServiceImpl implements ValidationService {

    private static ValidationService instance;

    public static synchronized ValidationService getInstance() {
        if (instance == null) {
            instance = new ValidationServiceImpl();
        }
        return instance;
    }

    @Override
    public void validateColumnName(String columnName, Map<String, Integer> columnPosition) {
        if (columnName == null || columnName.isEmpty()) {
            throw new RuntimeException("columnName cannot be null or empty");
        }
        if (!columnPosition.containsKey(columnName)) {
            throw new RuntimeException("unknown columnName::" + columnName);
        }
    }

    @Override
    public void validateRow(Map<String, String> row, int noOfCols, Map<String, Integer> columnPosition) {
        if (row == null || row.isEmpty()) {
            throw new RuntimeException("row to be inserted cannot be null or empty");
        }
        int noOfColsInRow = row.size();
        if (noOfColsInRow > noOfCols) {
            throw new RuntimeException("no of columns to be inserted or updated cannot be greater than no of " +
                    "columns in table::" + noOfCols);
        }
        for (String colName : row.keySet()) {
            if (!columnPosition.containsKey(colName)) {
                throw new RuntimeException("unknown columnName::" + colName);
            }
        }
    }
}
