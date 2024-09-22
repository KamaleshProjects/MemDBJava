package com.reflections.db.key.value;

public interface KeyValueDB {

    String get(String key);

    void set(String key, String value);

    void delete(String key);
}
