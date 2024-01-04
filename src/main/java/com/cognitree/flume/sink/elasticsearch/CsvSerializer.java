
package com.cognitree.flume.sink.elasticsearch;

import com.google.common.base.Charsets;
import lombok.extern.slf4j.Slf4j;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.ArrayList;
import java.util.List;

import static com.cognitree.flume.sink.elasticsearch.Constants.COLONS;
import static com.cognitree.flume.sink.elasticsearch.Constants.COMMA;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

@Slf4j
public class CsvSerializer extends AddHeaderSerializer {

    private static final String FIELDS = "csv.fields";
    private static final String DELIMITER = "csv.delimiter";
    private static final String DEFAULT_DELIMITER = "\t";

    private final List<String> names = new ArrayList<>();
    private final List<FieldType> types = new ArrayList<>();

    private String delimiter;

    @Override
    public XContentBuilder serialize(Event event) {
        try {
            XContentBuilder builder = jsonBuilder().startObject();
            //body
            String body = new String(event.getBody(), Charsets.UTF_8);
            String[] values = body.split(delimiter);
            for (int i = 0; i < names.size(); i++) {
                Util.addField(builder, names.get(i), values[i], types.get(i));
            }
            //headers
            addHeaders(event, builder);
            builder.endObject();
            return builder;
        } catch (Exception e) {
            return rawSerialize(event);
        }
    }

    @Override
    public void configure(Context context) {
        super.configure(context);
        String fields = context.getString(FIELDS);
        if (fields == null) {
            throw new RuntimeException("Fields for csv files are not configured," +
                    " please configured the property " + FIELDS);
        }
        try {
            delimiter = context.getString(DELIMITER, DEFAULT_DELIMITER);
            String[] fieldTypes = fields.split(COMMA);
            for (String fieldType : fieldTypes) {
                String[] parts = fieldType.split(COLONS);
                if (parts.length == 1) {
                    names.add(fieldType);
                    types.add(FieldType.STRING);
                } else {
                    names.add(parts[0]);
                    types.add(FieldType.get(parts[1]));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private XContentBuilder rawSerialize(Event event) {
        try {
            XContentBuilder builder = jsonBuilder().startObject();
            //body
            String body = new String(event.getBody(), Charsets.UTF_8);
            Util.addField(builder, "body", body, FieldType.STRING);
            //headers
            addHeaders(event, builder);
            builder.endObject();
            return builder;
        } catch (Exception e) {
            log.error("Error in serializing, event is: {}", Util.dump(event), e);
            return null;
        }
    }

}
