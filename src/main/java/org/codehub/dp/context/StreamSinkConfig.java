package org.codehub.dp.context;

public class StreamSinkConfig {

    /*
     * Kafka Sink
     */
    public static final String KAFKA_SINK_REQUIRED_BOOTSTRAP_SERVER = "kafka.server";
    public static final String KAFKA_SINK_REQUIRED_TOPIC = "kafka.topic";

    /*
     * Doris Sink
     */
    public static final String DORIS_SINK_REQUIRED_HOST = "doris.host";
    public static final String DORIS_SINK_REQUIRED_USERNAME = "doris.username";
    public static final String DORIS_SINK_REQUIRED_PASSWORD = "doris.password";
    public static final String DORIS_SINK_REQUIRED_DATABASE = "doris.database";
    public static final String DORIS_SINK_OPTIONAL_JDBC_PORT = "doris.jdbc.port";
    public static final String DORIS_SINK_OPTIONAL_HTTP_PORT = "doris.http.port";
    public static final String DORIS_SINK_OPTIONAL_SYNC_TABLES = "doris.sync.tables";
}
