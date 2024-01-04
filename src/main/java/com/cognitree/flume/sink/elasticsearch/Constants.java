package com.cognitree.flume.sink.elasticsearch;

/**
 * Class to configure properties and defaults
 */
public class Constants {

    public static final String COMMA = ",";
    public static final String COLONS = ":";

    public static final String INDEX = "index";
    public static final String TYPE = "type";
    public static final String ID = "id";

    public static final String ES_BULK_ACTIONS = "es.bulkActions";
    public static final Integer DEFAULT_ES_BULK_ACTIONS = 1000;

    public static final String ES_BULK_SIZE = "es.bulkSize";
    public static final String ES_BULK_SIZE_UNIT = "es.bulkSize.unit";
    public static final Integer DEFAULT_ES_BULK_SIZE = 5;

    public static final String ES_BULK_PROCESSOR_NAME = "es.bulkProcessor.name";
    public static final String DEFAULT_ES_BULK_PROCESSOR_NAME = "flume";

    public static final String ES_CONCURRENT_REQUEST = "es.concurrent.request";
    public static final Integer DEFAULT_ES_CONCURRENT_REQUEST = 1;

    public static final String ES_FLUSH_INTERVAL_TIME = "es.flush.interval.time";
    public static final String DEFAULT_ES_FLUSH_INTERVAL_TIME = "10s";

    public static final String ES_BACKOFF_POLICY_TIME_INTERVAL = "es.backoff.policy.time.interval";
    public static final String DEFAULT_ES_BACKOFF_POLICY_START_DELAY = "50M";

    public static final String ES_BACKOFF_POLICY_RETRIES = "es.backoff.policy.retries";
    public static final Integer DEFAULT_ES_BACKOFF_POLICY_RETRIES = 8;

    public static final String ES_INDEX = "es.index";
    public static final String DEFAULT_ES_INDEX = "default";

    public static final String ES_TYPE = "es.type";
    public static final String DEFAULT_ES_TYPE = "_doc";

    // Mandatory Properties
    public static final String ES_CLUSTER_NAME = "es.cluster.name";
    public static final String DEFAULT_CLUSTER_NAME = "elasticsearch";

    public static final String ES_HOSTS = "es.client.hosts";

    public static final Integer DEFAULT_ES_PORT = 9300;

    public static final String ES_CSV_FIELDS = "es.serializer.csv.fields";

    public static final String ES_AVRO_SCHEMA_FILE = "es.serializer.avro.schema.file";

    /**
     * This enum is used for the time unit
     * <p>
     * Time unit can be in Second, Minute or Mili second
     */
    public enum UnitEnum {
        SECOND("s"),
        MINUTE("m"),
        MILI_SECOND("M"),
        UNKNOWN("unknown");

        private String unit;

        UnitEnum(String unit) {
            this.unit = unit;
        }

        @Override
        public String toString() {
            return unit;
        }

        public static UnitEnum fromString(String unit) {
            for (UnitEnum unitEnum : UnitEnum.values()) {
                if (unitEnum.unit.equals(unit)) {
                    return unitEnum;
                }
            }
            return UNKNOWN;
        }
    }

    /**
     * This enum is used for unit of size of data
     * <p>
     * Unit can be in Mega byte or kilo byte
     */
    public enum ByteSizeEnum {
        MB("mb"),
        KB("kb");

        private String byteSizeUnit;

        ByteSizeEnum(String byteSizeUnit) {
            this.byteSizeUnit = byteSizeUnit;
        }

        @Override
        public String toString() {
            return byteSizeUnit;
        }
    }

    /**
     * Enum for field type
     */
    public enum FieldTypeEnum {
        STRING("string"),
        INT("int"),
        FLOAT("float"),
        BOOLEAN("boolean");

        private String fieldType;

        FieldTypeEnum(String fieldType) {
            this.fieldType = fieldType;
        }

        @Override
        public String toString() {
            return fieldType;
        }
    }
}
