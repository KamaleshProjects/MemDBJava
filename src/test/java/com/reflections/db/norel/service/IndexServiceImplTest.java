package com.reflections.db.norel.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexServiceImplTest {

    private final IndexService indexService = IndexServiceImpl.getInstance();

    @Test
    public void testCreateIndex_WithNullAndNonNullValues() {

    }

    @Test
    public void testInsertIndexIfPresent_NoInsertDueToIndexNotPresent() {
        Map<String, String> row = new HashMap<>();
        row.put("customer_id", "cust100");
        row.put("name", "Kamalesh S");
        Map<String, Map<String, List<Integer>>> indices = new HashMap<>();
        indexService.insertIndicesIfPresent(row, indices, null, 1);
        Assertions.assertEquals(0, indices.size());
    }

    @Test
    public void testInsertIndexIfPresent_InsertNonNullValue() {
        Map<String, String> row = new HashMap<>();
        row.put("customer_id", "cust100");
        row.put("name", "Kamalesh S");
        Map<String, Map<String, List<Integer>>> indices = new HashMap<>();
        Map<String, List<Integer>> nameIndex = new HashMap<>();
        List<Integer> rowList = new ArrayList<>();
        nameIndex.put("Kamalesh S", rowList);
        indices.put("name", nameIndex);
        indexService.insertIndicesIfPresent(row, indices, null, 1);
        Assertions.assertEquals(1, indices.get("name").get("Kamalesh S").size());
    }

    @Test
    public void testInsertIndexIfPresent_NullValue() {
        Map<String, String> row = new HashMap<>();
        row.put("customer_id", "cust100");
        row.put("name", null);
        Map<String, List<Integer>> nullIndices = new HashMap<>();
        nullIndices.put("name", new ArrayList<>());
        Map<String, Map<String, List<Integer>>> indices = new HashMap<>();
        Map<String, List<Integer>> nameIndex = new HashMap<>();
        List<Integer> rowList = new ArrayList<>();
        nameIndex.put("Kamalesh S", rowList);
        indices.put("name", nameIndex);
        indexService.insertIndicesIfPresent(row, indices, nullIndices, 1);
        Assertions.assertEquals(1, nullIndices.get("name").size());
    }
}
