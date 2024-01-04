package com.cognitree.flume.sink.elasticsearch;

import com.google.common.base.Throwables;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.elasticsearch.xcontent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static com.cognitree.flume.sink.elasticsearch.Constants.ES_AVRO_SCHEMA_FILE;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

/**
 * This Serializer assumes the event body to be in avro binary format
 */
public class AvroSerializer implements Serializer {

    private static final Logger logger = LoggerFactory.getLogger(AvroSerializer.class);

    private DatumReader<GenericRecord> datumReader;

    /**
     * Converts the avro binary data to the json format
     */
    @Override
    public XContentBuilder serialize(Event event) {
        XContentBuilder builder = null;
        try {
            if (datumReader != null) {
                Decoder decoder = new DecoderFactory().binaryDecoder(event.getBody(), null);
                GenericRecord data = datumReader.read(null, decoder);
                logger.trace("Record in event " + data);
                XContentParser parser = XContentFactory
                        .xContent(XContentType.JSON)
                        .createParser(NamedXContentRegistry.EMPTY,
                                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                                data.toString());
                builder = jsonBuilder().copyCurrentStructure(parser);
                parser.close();
            } else {
                logger.error("Schema File is not configured");
            }
        } catch (IOException e) {
            logger.error("Exception in parsing avro format data but continuing serialization to process further records", e);
        }
        return builder;
    }

    @Override
    public void configure(Context context) {
        String file = context.getString(ES_AVRO_SCHEMA_FILE);
        if (file == null) {
            Throwables.propagate(new Exception("Schema file is not configured, " +
                    "please configure the property " + ES_AVRO_SCHEMA_FILE));
        }
        try {
            Schema schema = new Schema.Parser().parse(new File(file));
            datumReader = new GenericDatumReader<>(schema);
        } catch (IOException e) {
            logger.error("Error in parsing schema file ", e);
            Throwables.propagate(e);
        }
    }
}
