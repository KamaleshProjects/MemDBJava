package com.reflections.db.norel.model;

import com.reflections.db.norel.service.IndexService;
import com.reflections.db.norel.service.IndexServiceImpl;
import com.reflections.db.norel.service.ValidationService;
import com.reflections.db.norel.service.ValidationServiceImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableImpl implements Table {

    private static final int FIRST_ROW = 0;

    public TableImpl(
            String name, List<String[]> rows, Map<String, Integer> columnPosition, int noOfCols, int noOfRows) {

        this.name = name;
        this.columnPosition = columnPosition;
        this.rows = rows;
        this.noOfCols = noOfCols;
        this.noOfRows = noOfRows;
        this.indices = new HashMap<>();
        this.nullIndices = new HashMap<>();

        this.indexService = IndexServiceImpl.getInstance();
        this.validationService = ValidationServiceImpl.getInstance();
    }

    private final String name;
    private final List<String[]> rows;
    private final Map<String, Integer> columnPosition;
    private final int noOfCols;
    private int noOfRows;
    private final Map<String, Map<String, List<Integer>>> indices;
    private final Map<String, List<Integer>> nullIndices;

    private final IndexService indexService;
    private final ValidationService validationService;

    @Override
    public void createIndex(String columnName) {
        validationService.validateColumnName(columnName, this.columnPosition);
        int colPos = this.columnPosition.get(columnName);
        this.indexService.createIndex(columnName, colPos, this.rows, this.noOfRows, this.indices, this.nullIndices);
    }

    @Override
    public void insertRow(Map<String, String> row) {
        this.validationService.validateRow(row, this.noOfCols, this.columnPosition);

        String[] newRow = new String[this.noOfCols];
        for (String colName: row.keySet()) {
            int colPos = this.columnPosition.get(colName);
            newRow[colPos] = row.get(colName);
        }
        this.rows.add(newRow);
        this.noOfRows++;

        this.indexService.insertIndicesIfPresent(row, this.indices, this.nullIndices, noOfRows - 1);
    }

    @Override
    public String[][] readRows(String columnName, String value) {
        this.validationService.validateColumnName(columnName, this.columnPosition);

        if (this.indices.containsKey(columnName)) {
            return this.indexService.readRowsViaIndex(columnName, value, this.rows, this.indices, this.nullIndices);
        }

        List<String[]> resultSet = new ArrayList<>();
        int colPos = this.columnPosition.get(columnName);

        if (value == null) {
            for (String[] row: this.rows) {
                if (row[colPos] == null) resultSet.add(row);
            }
        } else {
            for (String[] row: this.rows) {
                if (value.equals(row[colPos])) resultSet.add(row);
            }
        }

        String[][] result = new String[resultSet.size()][];
        for (int i = 0; i < resultSet.size(); i++) {
            result[i] = resultSet.get(i);
        }
        return result;
    }

    @Override
    public void updateRow(String columnName, String value, Map<String, String> updateValues) {
        this.validationService.validateColumnName(columnName, this.columnPosition);
        this.validationService.validateRow(updateValues, this.noOfCols, this.columnPosition);

        if (this.indices.containsKey(columnName)) {
            this.indexService.updateRowsViaIndex(
                    columnName, value, this.rows, this.indices, this.nullIndices, updateValues, this.columnPosition
            );
            return;
        }

        int colPos = this.columnPosition.get(columnName);

        if (value == null) {
            for (String[] row: this.rows) {
                if (row[colPos] == null) {
                    this.updateColumns(row, updateValues);
                }
            }
        } else {
            for (String[] row: this.rows) {
                if (value.equals(row[colPos])) {
                    this.updateColumns(row, updateValues);
                }
            }
        }
    }

    private void updateColumns(String[] row, Map<String, String> updateValues) {
        for (String colName: updateValues.keySet()) {
            int updateColPos = this.columnPosition.get(colName);
            row[updateColPos] = updateValues.get(colName);
        }
    }

    @Override
    public void deleteRow(String column, String value) {

    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String[] getColumns() {
        return this.rows.get(FIRST_ROW);
    }

    @Override
    public int getColCount() {
        return this.noOfCols;
    }

    @Override
    public int getRowCount() {
        return this.noOfRows;
    }
}
