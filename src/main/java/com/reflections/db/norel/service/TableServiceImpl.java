package com.reflections.db.norel.service;

import com.reflections.db.norel.model.Table;
import com.reflections.db.norel.model.TableImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableServiceImpl implements TableService {

    private static TableService instance;

    public static synchronized TableService getInstance() {
        if (instance == null) {
            instance = new TableServiceImpl();
        }
        return instance;
    }

    private static final int NEW_TABLE_SIZE = 0;

    @Override
    public Table createTable(String name, String[] columns) {
        if (name == null || name.isEmpty()) throw new RuntimeException("table name cannot be null or empty");
        if (columns.length == 0) {
            throw new RuntimeException("at least one column must be provided for creating a table");
        }
        for (String colName: columns) {
            if (colName == null || colName.isEmpty()) {
                throw new RuntimeException("column names cannot be null or empty");
            }
        }

        List<String[]> rows = new ArrayList<>();
        rows.add(columns);

        Map<String, Integer> columnPosition = new HashMap<>();
        int pos = 0;
        for (String columnName: columns) {
            columnPosition.put(columnName, pos++);
        }

        return new TableImpl(name, rows, columnPosition, pos, NEW_TABLE_SIZE);
    }
}
