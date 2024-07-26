package org.codehub.dp.util;

public enum DBType {

    MYSQL("mysql"), SQLSERVER("sqlserver");

    private String name;

    DBType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
