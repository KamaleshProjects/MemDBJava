package com.reflections.db.norel.service;

import java.util.List;
import java.util.Map;

public interface IndexService {

    void createIndex(
            String columnName, int colPos, List<String[]> rows, int noOfRows,
            Map<String, Map<String, List<Integer>>> indices,  Map<String, List<Integer>> nullIndices
    );

    void insertIndicesIfPresent(
            Map<String, String> row, Map<String, Map<String, List<Integer>>> indices,
            Map<String, List<Integer>> nullIndices, int rowIndex
    );

    String[][] readRowsViaIndex(
            String columnName, String value, List<String[]> rows, Map<String, Map<String, List<Integer>>> indices,
            Map<String, List<Integer>> nullIndices
    );

    void updateRowsViaIndex(
            String columnName, String value, List<String[]> rows, Map<String, Map<String, List<Integer>>> indices,
            Map<String, List<Integer>> nullIndices, Map<String, String> updateValues,
            Map<String, Integer> columnPosition
    );
}
