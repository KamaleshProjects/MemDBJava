package com.reflections.db.norel.service;

import java.util.Map;

public interface ValidationService {

    void validateColumnName(String columnName, Map<String, Integer> columnPosition);

    void validateRow(Map<String, String> row, int noOfCols, Map<String, Integer> columnPosition);
}
