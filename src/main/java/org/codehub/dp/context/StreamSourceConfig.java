package org.codehub.dp.context;

public class StreamSourceConfig {

    /*
     * Kafka Source
     */
    public static final String KAFKA_SOURCE_REQUIRED_BOOTSTRAP_SERVER = "kafka.server";
    public static final String KAFKA_SOURCE_REQUIRED_TOPIC = "kafka.topic";
    public static final String KAFKA_SOURCE_REQUIRED_GROUP = "kafka.group";
    public static final String KAFKA_SOURCE_OPTIONAL_OFFSET = "kafka.offset";
    public static final String KAFKA_SOURCE_DEFAULT_SOURCE_NAME = "Kafka Source";


    /*
     * CDC Source Common
     */
    public static final String CDC_SOURCE_COMMON_REQUIRED_HOSTNAME = "cdc.hostname";
    public static final String CDC_SOURCE_COMMON_REQUIRED_SCHEMA = "cdc.schema";
    public static final String CDC_SOURCE_COMMON_REQUIRED_DATABASE = "cdc.database";
    public static final String CDC_SOURCE_COMMON_REQUIRED_USERNAME = "cdc.username";
    public static final String CDC_SOURCE_COMMON_REQUIRED_PASSWORD = "cdc.password";
    public static final String CDC_SOURCE_COMMON_OPTIONAL_PORT = "cdc.port";
    public static final String CDC_SOURCE_COMMON_OPTIONAL_INCLUDING_TABLES = "cdc.including.tables";
    public static final String CDC_SOURCE_COMMON_OPTIONAL_INCLUDING_TABLE_COLUMNS_TEMPLATE = "cdc.including.*.columns";
    public static final String CDC_SOURCE_COMMON_OPTIONAL_EXCLUDING_TABLES = "cdc.excluding.tables";
    public static final String CDC_SOURCE_COMMON_OPTIONAL_SCAN_STARTUP_MODE = "cdc.scan.startup.mode";
    public static final String CDC_SOURCE_COMMON_OPTIONAL_SCAN_STARTUP_TIMESTAMP_MILLIS = "cdc.scan.startup.timestamp.millis";
    public static final String CDC_SOURCE_COMMON_OPTIONAL_ENABLE_SCHEMA_CHANGE = "cdc.enable.schema.change";
    public static final String CDC_SOURCE_COMMON_OPTIONAL_CONNECT_TIMEOUT = "cdc.connect.timeout";
    public static final String CDC_SOURCE_COMMON_OPTIONAL_CONNECT_MAX_RETRIES = "cdc.connect.max.retries";
    public static final String CDC_SOURCE_COMMON_OPTIONAL_CONNECTION_POOL_SIZE = "cdc.connection.pool.size";

    /*
     * CDC Source SQLSERVER
     */
    public static final String CDC_SOURCE_SQLSERVER_DEFAULT_SOURCE_NAME = "CDC SQLServer Source";

    /*
     * CDC Source MYSQL
     */
    public static final String CDC_SOURCE_MYSQL_REQUIRED_SERVER_ID = "cdc.mysql.server.id";
    public static final String CDC_SOURCE_MYSQL_DEFAULT_SOURCE_NAME = "CDC MYSQL Source";
}
