package com.reflections.db.norel.service;

import com.reflections.db.norel.model.Table;

public interface TableService {

    Table createTable(String name, String[] columns);
}
