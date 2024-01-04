package com.cognitree.flume.sink.elasticsearch;

import lombok.extern.slf4j.Slf4j;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.cognitree.flume.sink.elasticsearch.Constants.COMMA;

@Slf4j
public abstract class AddHeaderSerializer implements Serializer {
    public static final String HEADERS = "headers";
    public static final String NONE_HEADERS = "-";
    public static final String ALL_HEADERS = "*";

    protected ActionEnum action;

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

    protected void addHeaders(Event event, XContentBuilder contentBuilder) {
        Map<String, String> eventHeaders = event.getHeaders();
        switch (action) {
            case ALL:
                eventHeaders.keySet().forEach(k -> {
                    String value = eventHeaders.get(k);
                    addField(contentBuilder, k, value);
                });
                break;
            case INCLUDE:
                header2Type.keySet().forEach(k -> {
                    String value = eventHeaders.get(k);
                    addField(contentBuilder, k, value);
                });
                break;
            default:
                break;
        }
    }

    @Override
    public void configure(Context context) {
        String value = context.getString(HEADERS, NONE_HEADERS);
        value = value.trim();
        if (NONE_HEADERS.equals(value)) {
            action = ActionEnum.NONE;
        } else if (ALL_HEADERS.equals(value)) {
            action = ActionEnum.ALL;
        } else {
            action = ActionEnum.INCLUDE;
            for (String field : value.trim().split(COMMA)) {
                String[] parts = field.split(":");
                if (parts.length == 1) {
                    if (ALL_HEADERS.equals(field)) {
                        action = ActionEnum.ALL;
                    } else {
                        header2Type.put(field, FieldType.STRING);
                    }
                    continue;
                }
                if (parts.length != 2) {
                    throw new IllegalArgumentException("invalid fields: " + value);
                }
                header2Type.put(parts[0].trim(), FieldType.get(parts[1].trim()));
            }
        }
    }

    enum ActionEnum {
        ALL,
        NONE,
        INCLUDE
    }
}
