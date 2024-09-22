package com.reflections.db.norel.model;

import com.reflections.db.norel.service.TableService;
import com.reflections.db.norel.service.TableServiceImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class TableImplTest {

    private static final TableService tableService = TableServiceImpl.getInstance();

    @Test
    public void testTableInsert() {
        String tableName = "customers";
        int rowSize = 4;
        String[] columns = new String[rowSize];

        String customerIdCol = "customerId"; String nameCol = "name";
        String panCol = "pan"; String aadhaarCol = "aadhaar";

        columns[0] = customerIdCol; columns[1] = nameCol; columns[2] = panCol; columns[3] = aadhaarCol;

        Table customerTable = tableService.createTable(tableName, columns);

        String customerId = "CUST100"; String name = "Kamalesh";
        String pan = "HXIPK1134F";

        Map<String, String> row = new HashMap<>();
        row.put(customerIdCol, customerId); row.put(nameCol, name); row.put(panCol, pan);

        customerTable.insertRow(row);

        Assertions.assertEquals(1, customerTable.getRowCount());
    }

    @Test
    public void testTableFailToInsert_DueToUnknownColumn() {
        String tableName = "customers";
        int rowSize = 4;
        String[] columns = new String[rowSize];

        String customerIdCol = "customerId"; String nameCol = "name";
        String panCol = "pan"; String aadhaarCol = "aadhaar";

        columns[0] = customerIdCol; columns[1] = nameCol; columns[2] = panCol; columns[3] = aadhaarCol;

        Table customerTable = tableService.createTable(tableName, columns);

        Map<String, String> row = new HashMap<>();
        row.put("unknowncol", "garbageval");

        Assertions.assertThrows(RuntimeException.class, () -> customerTable.insertRow(row));
    }

    @Test
    public void testReadRows_NoMatchFound() {
        String tableName = "customers";
        int rowSize = 4;
        String[] columns = new String[rowSize];
        String customerIdCol = "customerId"; String nameCol = "name";
        String panCol = "pan"; String aadhaarCol = "aadhaar";
        columns[0] = customerIdCol; columns[1] = nameCol; columns[2] = panCol; columns[3] = aadhaarCol;
        Table customerTable = tableService.createTable(tableName, columns);
        String customerId = "CUST100"; String name = "Kamalesh";
        String pan = "HXIPK1134F";
        Map<String, String> row = new HashMap<>();
        row.put(customerIdCol, customerId); row.put(nameCol, name); row.put(panCol, pan);
        customerTable.insertRow(row);

        String[][] res = customerTable.readRows("customerId", "12");
        Assertions.assertEquals(0, res.length);
    }

    @Test
    public void testReadRows_WithoutIndexedColumn() {
        String tableName = "customers";
        int rowSize = 4;
        String[] columns = new String[rowSize];
        String customerIdCol = "customerId"; String nameCol = "name";
        String panCol = "pan"; String aadhaarCol = "aadhaar";
        columns[0] = customerIdCol; columns[1] = nameCol; columns[2] = panCol; columns[3] = aadhaarCol;
        Table customerTable = tableService.createTable(tableName, columns);
        String customerId = "CUST100"; String name = "Kamalesh";
        String pan = "HXIPK1134F";
        Map<String, String> row = new HashMap<>();
        row.put(customerIdCol, customerId); row.put(nameCol, name); row.put(panCol, pan);
        customerTable.insertRow(row);

        String[][] res = customerTable.readRows("customerId", "CUST100");
        Assertions.assertEquals("Kamalesh", res[0][1]);
    }

    @Test
    public void testReadRows_WithIndexedColumn() {
        String tableName = "customers";
        int rowSize = 4;
        String[] columns = new String[rowSize];
        String customerIdCol = "customerId"; String nameCol = "name";
        String panCol = "pan"; String aadhaarCol = "aadhaar";
        columns[0] = customerIdCol; columns[1] = nameCol; columns[2] = panCol; columns[3] = aadhaarCol;
        Table customerTable = tableService.createTable(tableName, columns);
        String customerId = "CUST100"; String name = "Kamalesh";
        String pan = "HXIPK1134F";
        Map<String, String> row = new HashMap<>();
        row.put(customerIdCol, customerId); row.put(nameCol, name); row.put(panCol, pan);
        customerTable.insertRow(row);

        customerTable.createIndex("customerId");

        String[][] res = customerTable.readRows("customerId", "CUST100");
        Assertions.assertEquals("Kamalesh", res[0][1]);
    }

    @Test
    public void testReadRows_WithoutIndexedColumnByNullValue() {
        String tableName = "customers";
        int rowSize = 4;
        String[] columns = new String[rowSize];
        String customerIdCol = "customerId"; String nameCol = "name";
        String panCol = "pan"; String aadhaarCol = "aadhaar";
        columns[0] = customerIdCol; columns[1] = nameCol; columns[2] = panCol; columns[3] = aadhaarCol;
        Table customerTable = tableService.createTable(tableName, columns);
        String customerId = null; String name = "Kamalesh";
        String pan = "HXIPK1134F";
        Map<String, String> row = new HashMap<>();
        row.put(customerIdCol, null); row.put(nameCol, name); row.put(panCol, pan);
        customerTable.insertRow(row);

        String[][] res = customerTable.readRows("customerId", null);
        Assertions.assertEquals("Kamalesh", res[0][1]);
    }

    @Test
    public void testReadRows_WithIndexedColumnByNullValue() {
        String tableName = "customers";
        int rowSize = 4;
        String[] columns = new String[rowSize];
        String customerIdCol = "customerId"; String nameCol = "name";
        String panCol = "pan"; String aadhaarCol = "aadhaar";
        columns[0] = customerIdCol; columns[1] = nameCol; columns[2] = panCol; columns[3] = aadhaarCol;
        Table customerTable = tableService.createTable(tableName, columns);
        String customerId = null; String name = "Kamalesh";
        String pan = "HXIPK1134F";
        Map<String, String> row = new HashMap<>();
        row.put(customerIdCol, null); row.put(nameCol, name); row.put(panCol, pan);
        customerTable.insertRow(row);

        customerTable.createIndex("customerId");

        String[][] res = customerTable.readRows("customerId", null);
        Assertions.assertEquals("Kamalesh", res[0][1]);
    }

    @Test
    public void testUpdateRows_WithIndex() {
        String tableName = "customers";
        int rowSize = 4;
        String[] columns = new String[rowSize];
        String customerIdCol = "customerId"; String nameCol = "name";
        String panCol = "pan"; String aadhaarCol = "aadhaar";
        columns[0] = customerIdCol; columns[1] = nameCol; columns[2] = panCol; columns[3] = aadhaarCol;
        Table customerTable = tableService.createTable(tableName, columns);
        String customerId = null; String name = "Kamalesh";
        String pan = "HXIPK1134F";
        Map<String, String> row = new HashMap<>();
        row.put(customerIdCol, null); row.put(nameCol, name); row.put(panCol, pan);
        customerTable.insertRow(row);
    }

    @Test
    public void testUpdateRows_WithoutIndex() {

    }
}
