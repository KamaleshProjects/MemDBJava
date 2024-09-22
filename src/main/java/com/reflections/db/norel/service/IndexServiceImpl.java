package com.reflections.db.norel.service;

import java.util.*;

public class IndexServiceImpl implements IndexService {

    private static IndexService instance;

    public static synchronized IndexService getInstance() {
        if (instance == null) {
            instance = new IndexServiceImpl();
        }
        return instance;
    }

    @Override
    public void createIndex(
            String columnName, int colPos, List<String[]> rows, int noOfRows,
            Map<String, Map<String, List<Integer>>> indices, Map<String, List<Integer>> nullIndices) {

        if (indices.containsKey(columnName)) return;

        Map<String, List<Integer>> colIndex = new HashMap<>();
        List<Integer> nullIndex = new ArrayList<>();
        for (int i = 0; i < noOfRows; i++) {
            String[] row = rows.get(i + 1);
            String value = row[colPos];

            if (value == null) {
                nullIndex.add(i + 1);
                continue;
            }

            List<Integer> matchRows = colIndex.get(value);
            if (matchRows != null) {
                matchRows.add(i);
                continue;
            }

            matchRows = new ArrayList<>();
            matchRows.add(i + 1);
            colIndex.put(value, matchRows);
        }

        indices.put(columnName, colIndex);
        nullIndices.put(columnName, nullIndex);
    }

    @Override
    public void insertIndicesIfPresent(
            Map<String, String> row, Map<String, Map<String, List<Integer>>> indices,
            Map<String, List<Integer>> nullIndices, int rowIndex) {

        for (String colName: row.keySet()) {
            String val = row.get(colName);
            this.insertIndexIfPresent(colName, val, indices, nullIndices, rowIndex);
        }
    }

    @Override
    public String[][] readRowsViaIndex(
            String columnName, String value, List<String[]> rows, Map<String, Map<String, List<Integer>>> indices,
            Map<String, List<Integer>> nullIndices) {

        if (value == null) {
            return this.fetchResultBasisIndex(columnName, nullIndices, rows);
        }
        Map<String, List<Integer>> valueIndex = indices.get(columnName);
        return this.fetchResultBasisIndex(value, valueIndex, rows);
    }

    @Override
    public void updateRowsViaIndex(
            String columnName, String value, List<String[]> rows, Map<String, Map<String, List<Integer>>> indices,
            Map<String, List<Integer>> nullIndices, Map<String, String> updateValues,
            Map<String, Integer> columnPosition) {

        if (value == null) {
            this.updateRowBasisIndex(columnName, nullIndices, rows, updateValues, columnPosition);
            return;
        }
        Map<String, List<Integer>> valIndex = indices.get(columnName);
        this.updateRowBasisIndex(value, valIndex, rows, updateValues, columnPosition);
    }

    private void updateRowBasisIndex(
            String value, Map<String, List<Integer>> index, List<String[]> rows, Map<String, String> updateValues,
            Map<String, Integer> columnPosition) {

        List<Integer> matchRowList = index.get(value);
        if (matchRowList.isEmpty()) return;

        for (int rowPointer: matchRowList) {
            String[] row = rows.get(rowPointer);
            for (String colName: updateValues.keySet()) {
                int updateColPos = columnPosition.get(colName);
                row[updateColPos] = updateValues.get(colName);
            }
        }
    }

    public String[][] fetchResultBasisIndex(String value, Map<String, List<Integer>> index, List<String[]> rows) {

        List<Integer> matchRowList = index.get(value);
        if (matchRowList.isEmpty()) return new String[0][0];

        List<String[]> resultSet = new ArrayList<>();
        for (int row: matchRowList) {
            resultSet.add(rows.get(row));
        }

        String[][] result = new String[resultSet.size()][];
        for (int i = 0; i < resultSet.size(); i++) {
            result[i] = resultSet.get(i);
        }
        return result;
    }

    public void insertIndexIfPresent(
            String columnName, String value, Map<String, Map<String, List<Integer>>> indices,
            Map<String, List<Integer>> nullIndices, int rowIndex) {

        if (!indices.containsKey(columnName)) return;

        if (value == null) {
            List<Integer> nullIndex = nullIndices.get(columnName);
            nullIndex.add(rowIndex);
            return;
        }

        Map<String, List<Integer>> index = indices.get(columnName);
        List<Integer> valIndex = index.get(value);
        if (valIndex != null) {
            valIndex.add(rowIndex);
            return;
        }
        valIndex = new ArrayList<>();
        valIndex.add(rowIndex);
        index.put(value, valIndex);
    };
}
