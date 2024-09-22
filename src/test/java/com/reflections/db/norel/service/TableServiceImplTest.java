package com.reflections.db.norel.service;

import com.reflections.db.norel.model.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class TableServiceImplTest {

    private static final TableService tableService = TableServiceImpl.getInstance();

    @Test
    public void testCreateTable() {

        String tableName = "customers";
        int rowSize = 4;
        String[] columns = new String[rowSize];
        columns[0] = "customer_id";
        columns[1] = "name";
        columns[2] = "pan";
        columns[3] = "aadhaar";

        Table customerTable = tableService.createTable(tableName, columns);

        Assertions.assertEquals(tableName, customerTable.getName());
        boolean columnsMatching = Arrays.equals(columns, customerTable.getColumns());
        Assertions.assertTrue(columnsMatching);
        Assertions.assertEquals(rowSize, customerTable.getColCount());
        Assertions.assertEquals(0, customerTable.getRowCount());
    }

    @Test
    public void testFailToCreateTable_WithEmptyName() {
        String tableName = "";
        int rowSize = 4;
        String[] columns = new String[rowSize];
        columns[0] = "customer_id";
        columns[1] = "name";
        columns[2] = "pan";
        columns[3] = "aadhaar";

        Assertions.assertThrows(RuntimeException.class, () -> tableService.createTable(tableName, columns));
    }

}
