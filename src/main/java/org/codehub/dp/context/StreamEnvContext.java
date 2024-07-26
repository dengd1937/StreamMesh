package org.codehub.dp.context;

import org.codehub.dp.cdc.serialize.JsonDebeziumDeserializationSchema;
import org.codehub.dp.cdc.serialize.JsonKeySerializationSchema;
import org.codehub.dp.cdc.serialize.JsonDorisRecordSerializer;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceBuilder;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.StringUtils;
import org.codehub.dp.util.DBType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.codehub.dp.context.StreamSinkConfig.*;
import static org.codehub.dp.context.StreamSourceConfig.*;

public class StreamEnvContext {

    public static void jobExecute(StreamExecutionEnvironment env) throws Exception {
        env.execute(Thread.currentThread().getStackTrace()[2].getClassName());
    }

    /* ================================================kafka source================================================== */
    public static DataStreamSource<String> getKafkaSource(
            StreamExecutionEnvironment env, String[] args) {
        ParameterTool from = ParameterTool.fromArgs(args);
        return getKafkaSource(env, from, null);
    }

    public static DataStreamSource<String> getKafkaSource(
            StreamExecutionEnvironment env, String configFilepath) throws IOException {
        ParameterTool from = ParameterTool.fromPropertiesFile(configFilepath);
        return getKafkaSource(env, from, null);
    }

    public static DataStreamSource<String> getKafkaSource(
            StreamExecutionEnvironment env, String[] args, Duration duration) {
        ParameterTool from = ParameterTool.fromArgs(args);
        return getKafkaSource(env, from, WatermarkStrategy.forBoundedOutOfOrderness(duration));
    }

    public static DataStreamSource<String> getKafkaSource(
            StreamExecutionEnvironment env, String configFilepath, Duration duration) throws IOException {
        ParameterTool from = ParameterTool.fromPropertiesFile(configFilepath);
        return getKafkaSource(env, from, WatermarkStrategy.forBoundedOutOfOrderness(duration));
    }

    private static DataStreamSource<String> getKafkaSource(
            StreamExecutionEnvironment env, ParameterTool params,
            WatermarkStrategy<String> watermarkStrategy) {
        String server = params.getRequired(KAFKA_SOURCE_REQUIRED_BOOTSTRAP_SERVER);
        String topic = params.getRequired(KAFKA_SOURCE_REQUIRED_TOPIC);
        String group = params.getRequired(KAFKA_SOURCE_REQUIRED_GROUP);
        OffsetsInitializer offsetInit = params.has(KAFKA_SOURCE_OPTIONAL_OFFSET)
                ? OffsetsInitializer.timestamp(params.getLong(KAFKA_SOURCE_OPTIONAL_OFFSET))
                : OffsetsInitializer.latest();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(server)
                .setTopics(topic)
                .setGroupId(group)
                .setStartingOffsets(offsetInit)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        return env.fromSource(source,
                watermarkStrategy == null ? WatermarkStrategy.noWatermarks() : watermarkStrategy, KAFKA_SOURCE_DEFAULT_SOURCE_NAME);
    }

    public static DataStream<String> getCdcSource(StreamExecutionEnvironment env, String configFilepath) throws Exception {
        ParameterTool params = ParameterTool.fromPropertiesFile(configFilepath);
        String dbType = params.getRequired(CDC_SOURCE_COMMON_REQUIRED_DB_TYPE);
        if (dbType.equalsIgnoreCase(DBType.MYSQL.getName())) {
            return getMysqlCdcSource(env, params);
        } else if (dbType.equalsIgnoreCase(DBType.SQLSERVER.getName())) {
            return getSqlServerCdcSource(env, params);
        } else {
            throw new Exception("Currently does not support related database types：" + dbType);
        }
    }

    /* ============================================sqlserver cdc source============================================== */
    public static DataStream<String> getSqlServerCdcSource(StreamExecutionEnvironment env, String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        return getSqlServerCdcSource(env, params);
    }

    public static DataStream<String> getSqlServerCdcSource(StreamExecutionEnvironment env, String configFilepath) throws Exception {
        ParameterTool params = ParameterTool.fromPropertiesFile(configFilepath);
        return getSqlServerCdcSource(env, params);
    }

    private static DataStream<String> getSqlServerCdcSource(StreamExecutionEnvironment env, ParameterTool params) throws Exception {
        String hostname = params.getRequired(CDC_SOURCE_COMMON_REQUIRED_HOSTNAME);
        int port = Integer.parseInt(params.get(CDC_SOURCE_COMMON_OPTIONAL_PORT, "1433"));
        String username = params.getRequired(CDC_SOURCE_COMMON_REQUIRED_USERNAME);
        String password = params.getRequired(CDC_SOURCE_COMMON_REQUIRED_PASSWORD);
        String database = params.getRequired(CDC_SOURCE_COMMON_REQUIRED_DATABASE);
        String schema = params.getRequired(CDC_SOURCE_COMMON_REQUIRED_SCHEMA);

        String scanStartupMode = params.get(CDC_SOURCE_COMMON_OPTIONAL_SCAN_STARTUP_MODE, "initial");
        String includingTables = params.get(CDC_SOURCE_COMMON_OPTIONAL_INCLUDING_TABLES, null);
        String excludingTables = params.get(CDC_SOURCE_COMMON_OPTIONAL_EXCLUDING_TABLES, null);
        String enableSchemaChange = "false";

        StartupOptions startupOptions = StartupOptions.initial();
        if (scanStartupMode.equalsIgnoreCase("initial")) {
            startupOptions = StartupOptions.initial();
        } else if (scanStartupMode.equalsIgnoreCase("latest-offset")) {
            startupOptions = StartupOptions.latest();
        }

        String sourceTables = getSourceTables(schema, includingTables, excludingTables);
        Properties debeziumeProperties = new Properties();
        debeziumeProperties.setProperty("debezium.database.history.store.only.captured.tables.ddl", enableSchemaChange);
        SqlServerSourceBuilder.SqlServerIncrementalSource<String> sqlServerIncrementalSource = new SqlServerSourceBuilder<String>()
                .hostname(hostname)
                .port(port)
                .username(username)
                .password(password)
                .databaseList(database)
                .tableList(sourceTables)
                .startupOptions(startupOptions)
                .debeziumProperties(debeziumeProperties)
                .deserializer(new JsonDebeziumDeserializationSchema(params.getConfiguration()))
                .includeSchemaChanges(false)
                .build();


        return env.fromSource(sqlServerIncrementalSource, WatermarkStrategy.noWatermarks(), CDC_SOURCE_SQLSERVER_DEFAULT_SOURCE_NAME);
    }

    /* ===============================================mysql cdc source================================================= */
    public static DataStream<String> getMysqlCdcSource(StreamExecutionEnvironment env, String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        return getMysqlCdcSource(env, params);
    }

    public static DataStream<String> getMysqlCdcSource(StreamExecutionEnvironment env, String configFilepath) throws Exception {
        ParameterTool params = ParameterTool.fromPropertiesFile(configFilepath);
        return getMysqlCdcSource(env, params);
    }

    private static DataStream<String> getMysqlCdcSource(StreamExecutionEnvironment env, ParameterTool params) throws Exception {
        String hostname = params.getRequired(CDC_SOURCE_COMMON_REQUIRED_HOSTNAME);
        int port = params.getInt(CDC_SOURCE_COMMON_OPTIONAL_PORT, MySqlSourceOptions.PORT.defaultValue());
        String username = params.getRequired(CDC_SOURCE_COMMON_REQUIRED_USERNAME);
        String password = params.getRequired(CDC_SOURCE_COMMON_REQUIRED_PASSWORD);
        String database = params.getRequired(CDC_SOURCE_COMMON_REQUIRED_DATABASE);
        String serverId = params.getRequired(CDC_SOURCE_MYSQL_REQUIRED_SERVER_ID);

        String scanStartupMode = params.get(CDC_SOURCE_COMMON_OPTIONAL_SCAN_STARTUP_MODE, "initial");
        String includingTables = params.get(CDC_SOURCE_COMMON_OPTIONAL_INCLUDING_TABLES, null);
        String excludingTables = params.get(CDC_SOURCE_COMMON_OPTIONAL_EXCLUDING_TABLES, null);
        boolean enableSchemaChange = params.getBoolean(CDC_SOURCE_COMMON_OPTIONAL_ENABLE_SCHEMA_CHANGE, false);
        long connectTimeout = params.getLong(CDC_SOURCE_COMMON_OPTIONAL_CONNECT_TIMEOUT,
                MySqlSourceOptions.CONNECT_TIMEOUT.defaultValue().getSeconds());
        int connectMaxReties = params.getInt(CDC_SOURCE_COMMON_OPTIONAL_CONNECT_MAX_RETRIES,
                MySqlSourceOptions.CONNECT_MAX_RETRIES.defaultValue());
        int connectPoolSize = params.getInt(CDC_SOURCE_COMMON_OPTIONAL_CONNECTION_POOL_SIZE,
                MySqlSourceOptions.CONNECTION_POOL_SIZE.defaultValue());

        org.apache.flink.cdc.connectors.mysql.table.StartupOptions startupOptions = org.apache.flink.cdc.connectors.mysql.table.StartupOptions.initial();
        if (scanStartupMode.equalsIgnoreCase("initial")) {
            startupOptions = org.apache.flink.cdc.connectors.mysql.table.StartupOptions.initial();
        } else if (scanStartupMode.equalsIgnoreCase("latest-offset")) {
            startupOptions = org.apache.flink.cdc.connectors.mysql.table.StartupOptions.latest();
        } else if (scanStartupMode.equalsIgnoreCase("earliest-offset")) {
            startupOptions = org.apache.flink.cdc.connectors.mysql.table.StartupOptions.earliest();
        } else if (scanStartupMode.equalsIgnoreCase("timestamp")) {
            startupOptions = org.apache.flink.cdc.connectors.mysql.table.StartupOptions.timestamp(
                    params.getLong(CDC_SOURCE_COMMON_OPTIONAL_SCAN_STARTUP_TIMESTAMP_MILLIS));
        }

        String sourceTables = getSourceTables(database, includingTables, excludingTables);
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(hostname)
                .port(port)
                .username(username)
                .password(password)
                .serverId(serverId)
                .databaseList(database)
                .tableList(sourceTables)
                .startupOptions(startupOptions)
                .serverTimeZone("Asia/Shanghai")
                .includeSchemaChanges(enableSchemaChange)
                .connectTimeout(Duration.ofSeconds(connectTimeout))
                .connectMaxRetries(connectMaxReties)
                .connectionPoolSize(connectPoolSize)
                .deserializer(new JsonDebeziumDeserializationSchema(params.getConfiguration()))
                .scanNewlyAddedTableEnabled(true)
                .build();

        return env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), CDC_SOURCE_MYSQL_DEFAULT_SOURCE_NAME);
    }

    private static String getSourceTables(String tablePrefix, String includingTables, String excludingTables) {
        String sourceTables;
        if (includingTables == null) {
            includingTables = ".*";
        }
        String includingPattern =
                String.format("(%s)\\.(%s)", tablePrefix, includingTables);
        if (StringUtils.isNullOrWhitespaceOnly(excludingTables)) {
            sourceTables = includingPattern;
        } else {
            String excludingPattern =
                    String.format("?!(%s\\.(%s))$", tablePrefix, excludingTables);
            sourceTables = String.format("(%s)(%s)", includingPattern, excludingPattern);
        }
        return sourceTables;
    }

    /* ==================================================sink======================================================== */
    public static Sink<String> getKafkaSink(String[] args) {
        ParameterTool from = ParameterTool.fromArgs(args);
        return getKafkaSink(from, null);
    }

    public static Sink<String> getKafkaSink(String[] args, List<String> keyFieldPaths) {
        ParameterTool from = ParameterTool.fromArgs(args);
        return getKafkaSink(from, keyFieldPaths);
    }

    public static Sink<String> getKafkaSink(String configFilepath) throws IOException {
        ParameterTool from = ParameterTool.fromPropertiesFile(configFilepath);
        return getKafkaSink(from, null);
    }

    public static Sink<String> getKafkaSink(String configFilepath, List<String> keyFieldPaths) throws IOException {
        ParameterTool from = ParameterTool.fromPropertiesFile(configFilepath);
        return getKafkaSink(from, keyFieldPaths);
    }

    private static Sink<String> getKafkaSink(ParameterTool params, List<String> keyFieldPaths) {
        String server = params.getRequired(KAFKA_SINK_REQUIRED_BOOTSTRAP_SERVER);
        String topic = params.getRequired(KAFKA_SINK_REQUIRED_TOPIC);

        Properties sinkPro = new Properties();
        sinkPro.put("transaction.timeout.ms", 15 * 60 * 1000);

        KafkaRecordSerializationSchemaBuilder<String> schemaBuilder = KafkaRecordSerializationSchema.builder()
                .setTopic(topic)
                .setValueSerializationSchema(new SimpleStringSchema());
        if (keyFieldPaths != null && !keyFieldPaths.isEmpty()) {
            schemaBuilder.setKeySerializationSchema(new JsonKeySerializationSchema(keyFieldPaths));
        }

        return KafkaSink.<String>builder()
                .setBootstrapServers(server)
                .setRecordSerializer(schemaBuilder.build())
                .setKafkaProducerConfig(sinkPro)
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();
    }

    public static Sink<String> buildDorisSink(String configFilepath, String table) throws IOException {
        ParameterTool from = ParameterTool.fromPropertiesFile(configFilepath);
        return buildDorisSink(from, table);
    }

    private static Sink<String> buildDorisSink(ParameterTool params, String table) {
        String host = params.getRequired(DORIS_SINK_REQUIRED_HOST);
        int httpPort = Integer.parseInt(params.get(DORIS_SINK_OPTIONAL_HTTP_PORT, "8030"));
        String database = params.getRequired(DORIS_SINK_REQUIRED_DATABASE);
        String username = params.getRequired(DORIS_SINK_REQUIRED_USERNAME);
        String password = params.getRequired(DORIS_SINK_REQUIRED_PASSWORD);

        DorisSink.Builder<String> builder = DorisSink.builder();
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        String tbl = database + "." + table;
        dorisBuilder.setFenodes(host + ":" + httpPort)
                .setTableIdentifier(tbl)
                .setUsername(username)
                .setPassword(password);
        Properties pro = new Properties();
        //json data format
        pro.setProperty("format", "json");
        pro.setProperty("read_json_by_line", "true");
        // 解决精度丢失
        pro.setProperty("num_as_string", "true");

        //delete use
        pro.setProperty("sink.enable-delete", "true");
        DorisExecutionOptions executionOptions = DorisExecutionOptions.builder()
                .setLabelPrefix("label-" + UUID.randomUUID() + table) //streamload label prefix,
                .setStreamLoadProp(pro)
                .setDeletable(true)
                .build();
        DorisOptions dorisOptions = dorisBuilder.build();
        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionOptions)
                .setSerializer(new JsonDorisRecordSerializer(dorisOptions)) //serialize according to string
                .setDorisOptions(dorisOptions);

        return builder.build();
    }

}
