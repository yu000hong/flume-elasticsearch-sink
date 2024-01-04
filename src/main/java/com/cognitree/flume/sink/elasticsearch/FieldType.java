package com.cognitree.flume.sink.elasticsearch;

public enum FieldType {
    STRING("string"),
    INT("int"),
    LONG("long"),
    FLOAT("float"),
    DOUBLE("double"),
    BOOLEAN("boolean"),
    DATE("date"),
    TIMESTAMP("timestamp");

    private final String type;

    FieldType(String type) {
        this.type = type;
    }

    public static FieldType get(String type) {
        if (type == null) {
            return null;
        }
        return FieldType.valueOf(type.toUpperCase());
    }

    @Override
    public String toString() {
        return type;
    }

}
