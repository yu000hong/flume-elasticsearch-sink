package com.cognitree.flume.sink.elasticsearch;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public abstract class AbstractSerializer implements Serializer {
    private static final String HEADERS = "headers";

    private final Map<String, FieldType> header2Type = new HashMap<>();

    protected FieldType getHeaderType(String field) {
        return header2Type.get(field);
    }

    protected void addField(XContentBuilder contentBuilder, String field, String value) {
        if (value == null) {
            return;
        }
        FieldType type = header2Type.get(field);
        try {
            Util.addField(contentBuilder, field, value, type);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public void configure(Context context) {
        String value = context.getString(HEADERS);
        if (!StringUtils.isBlank(value)) {
            for (String field : value.trim().split(",")) {
                String[] parts = field.split(":");
                if (parts.length != 2) {
                    throw new IllegalArgumentException("invalid fields: " + value);
                }
                header2Type.put(parts[0].trim(), FieldType.get(parts[1].trim()));
            }
        }
    }

}
