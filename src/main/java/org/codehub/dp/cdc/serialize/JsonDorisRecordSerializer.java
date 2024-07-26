package org.codehub.dp.cdc.serialize;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.exception.IllegalArgumentException;
import org.apache.doris.flink.sink.schema.SchemaChangeHelper;
import org.apache.doris.flink.sink.schema.SchemaChangeManager;
import org.apache.doris.flink.sink.writer.EventType;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.doris.flink.tools.cdc.SourceConnector;
import org.apache.doris.flink.tools.cdc.mysql.MysqlType;
import org.apache.doris.flink.tools.cdc.oracle.OracleType;
import org.apache.doris.flink.tools.cdc.postgres.PostgresType;
import org.apache.doris.flink.tools.cdc.sqlserver.SqlServerType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class JsonDorisRecordSerializer implements DorisRecordSerializer<String> {
    private static final Logger LOG = LoggerFactory.getLogger(JsonDorisRecordSerializer.class);
    private static final String OP_READ = "r"; // snapshot read
    private static final String OP_CREATE = "c"; // insert
    private static final String OP_UPDATE = "u"; // update
    private static final String OP_DELETE = "d"; // delete
    private static final Pattern addDropDDLPattern = Pattern.compile(
            "ALTER\\s+TABLE\\s+[^\\s]+\\s+(ADD|DROP)\\s+(COLUMN\\s+)?([^\\s]+)(\\s+([^\\s]+))?.*", Pattern.CASE_INSENSITIVE);
    private static final Pattern renameDDLPattern = Pattern.compile(
            "ALTER\\s+TABLE\\s+(\\w+)\\s+RENAME\\s+COLUMN\\s+(\\w+)\\s+TO\\s+(\\w+)", Pattern.CASE_INSENSITIVE);
    private ObjectMapper objectMapper = new ObjectMapper();
    private DorisOptions dorisOptions;
    private boolean firstSchemaChange;
    private Map<String, FieldSchema> originFieldSchemaMap;
    private SourceConnector sourceConnector;
    private SchemaChangeManager schemaChangeManager;

    public static final String EXECUTE_DDL = "ALTER TABLE %s %s COLUMN %s %s"; // alter table tbl add cloumn aca int

    public JsonDorisRecordSerializer(DorisOptions dorisOptions) {
        this.dorisOptions = dorisOptions;
        // Prevent loss of decimal data precision
        this.objectMapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(true);
        this.objectMapper.setNodeFactory(jsonNodeFactory);
        this.firstSchemaChange = true;
        this.schemaChangeManager = new SchemaChangeManager(dorisOptions);
    }

    @Override
    public DorisRecord serialize(String record) throws IOException {
        LOG.warn("received debezium json data: {}", record);
        JsonNode recordRoot = objectMapper.readValue(record, JsonNode.class);
        String op = extractString(recordRoot, "op");
        if (Objects.isNull(op)) {
            // schema change ddl
            schemaChange(recordRoot);
            return null;
        }

        String dorisTableIdentifier = dorisOptions.getTableIdentifier();
        Map<String, Object> valueMap = extractDataRow(recordRoot);
        if (Arrays.asList(OP_CREATE, OP_READ, OP_UPDATE).contains(op)) {
            valueMap.put("__DORIS_DELETE_SIGN__", false);
        } else if (OP_DELETE.equals(op)){
            valueMap.put("__DORIS_DELETE_SIGN__", true);
        } else {
            LOG.warn("parse record fail, unknown op {} in {}", op, record);
        }

        return DorisRecord.of(dorisTableIdentifier, objectMapper.writeValueAsString(valueMap).getBytes(StandardCharsets.UTF_8));
    }

    protected EventType extractEventType(JsonNode record) throws JsonProcessingException {
        JsonNode tableChange = extractTableChange(record);
        if(tableChange == null || tableChange.get("type") == null){
            return null;
        }
        String type = tableChange.get("type").asText();
        if(EventType.ALTER.toString().equalsIgnoreCase(type)){
            return EventType.ALTER;
        }else if(EventType.CREATE.toString().equalsIgnoreCase(type)){
            return EventType.CREATE;
        }
        return null;
    }

    protected JsonNode extractTableChange(JsonNode record) throws JsonProcessingException {
        JsonNode historyRecord = extractJsonNode(record, "historyRecord");
        JsonNode tableChanges = historyRecord.get("tableChanges");
        if(!Objects.isNull(tableChanges)){
            return tableChanges.get(0);
        }
        return null;
    }

    protected Tuple2<String, String> getDorisTableTuple(){
        String identifier = dorisOptions.getTableIdentifier();
        if(StringUtils.isNullOrWhitespaceOnly(identifier)){
            return null;
        }
        String[] tableInfo = identifier.split("\\.");
        if(tableInfo.length != 2){
            return null;
        }
        return Tuple2.of(tableInfo[0], tableInfo[1]);
    }

    private void schemaChange(JsonNode recordRoot) {
        EventType eventType;
        try {
            eventType = extractEventType(recordRoot);
            if (eventType == null) {
                return;
            }
            if (eventType.equals(EventType.ALTER)){
                // db,table
                Tuple2<String, String> tuple = getDorisTableTuple();
                if(tuple == null){
                    return;
                }
                List<String> ddlSqlList = extractDDLList(recordRoot);
                if (CollectionUtils.isEmpty(ddlSqlList)) {
                    LOG.warn("ddl can not do schema change:{}", recordRoot);
                    return;
                }
                List<SchemaChangeHelper.DDLSchema> ddlSchemas = SchemaChangeHelper.getDdlSchemas();
                for (int i = 0; i < ddlSqlList.size(); i++) {
                    SchemaChangeHelper.DDLSchema ddlSchema = ddlSchemas.get(i);
                    String ddlSql = ddlSqlList.get(i);
                    boolean doSchemaChange = checkSchemaChange(tuple.f0, tuple.f1, ddlSchema);
                    boolean status = doSchemaChange && schemaChangeManager.execute(ddlSql, tuple.f0);
                    LOG.warn("schema change status:{}, ddl:{}", status, ddlSql);
                }
            } else {
                LOG.warn("Unsupported event type {}", eventType);
            }
        }catch (Exception ex) {
            LOG.warn("schema change error :", ex);
        }
    }

    private List<String> extractDDLList(JsonNode record) throws IOException {
        String dorisTable = dorisOptions.getTableIdentifier();
        JsonNode historyRecord = extractJsonNode(record, "historyRecord");
        String ddl = extractString(historyRecord, "ddl");
        JsonNode tableChange = extractTableChange(record);
        if (Objects.isNull(tableChange) || Objects.isNull(ddl)) {
            return null;
        }
        JsonNode columns = tableChange.get("table").get("columns");
        if (firstSchemaChange) {
            sourceConnector = SourceConnector.valueOf(record.get("connector").asText().toUpperCase());
            originFieldSchemaMap = new LinkedHashMap<>();
            columns.forEach(column -> buildFieldSchema(originFieldSchemaMap, column));
        }
        firstSchemaChange = false;

        // rename ddl
        Matcher renameMatcher = renameDDLPattern.matcher(ddl);
        if (renameMatcher.find()) {
            String oldColumnName = renameMatcher.group(2);
            String newColumnName = renameMatcher.group(3);
            return SchemaChangeHelper.generateRenameDDLSql(
                    dorisTable, oldColumnName, newColumnName, originFieldSchemaMap);
        }
        // add/drop ddl
        Map<String, FieldSchema> updateFiledSchema = new LinkedHashMap<>();
        for (JsonNode column : columns) {
            buildFieldSchema(updateFiledSchema, column);
        }
        SchemaChangeHelper.compareSchema(updateFiledSchema, originFieldSchemaMap);
        // In order to avoid other source table column change operations other than add/drop/rename,
        // which may lead to the accidental deletion of the doris column.
        Matcher matcher = addDropDDLPattern.matcher(ddl);
        if (!matcher.find()) {
            return null;
        }
        return SchemaChangeHelper.generateDDLSql(dorisTable);
    }

    private void buildFieldSchema(Map<String, FieldSchema> filedSchemaMap, JsonNode column) {
        String fieldName = column.get("name").asText();
        String dorisTypeName = buildDorisTypeName(column);
        String defaultValue = handleDefaultValue(extractString(column, "defaultValueExpression"));
        String comment = extractString(column, "comment");
        filedSchemaMap.put(fieldName, new FieldSchema(fieldName, dorisTypeName, defaultValue, comment));
    }

    private String handleDefaultValue(String defaultValue) {
        if (StringUtils.isNullOrWhitespaceOnly(defaultValue)) {
            return null;
        }
        // Due to historical reasons, doris needs to add quotes to the default value of the new column
        // For example in mysql: alter table add column c1 int default 100
        // In Doris: alter table add column c1 int default '100'
        if (Pattern.matches("['\"].*?['\"]", defaultValue)) {
            return defaultValue;
        } else if (defaultValue.equals("1970-01-01 00:00:00")) {
            // TODO: The default value of setting the current time in CDC is 1970-01-01 00:00:00
            return "current_timestamp";
        }
        return "'" + defaultValue + "'";
    }

    private String buildDorisTypeName(JsonNode column) {
        int length = column.get("length") == null ? 0 : column.get("length").asInt();
        int scale = column.get("scale") == null ? 0 : column.get("scale").asInt();
        String sourceTypeName = column.get("typeName").asText();
        String dorisTypeName;
        switch (sourceConnector) {
            case MYSQL:
                dorisTypeName = MysqlType.toDorisType(sourceTypeName, length, scale);
                break;
            case ORACLE:
                dorisTypeName = OracleType.toDorisType(sourceTypeName, length, scale);
                break;
            case POSTGRES:
                dorisTypeName = PostgresType.toDorisType(sourceTypeName, length, scale);
                break;
            case SQLSERVER:
                dorisTypeName = SqlServerType.toDorisType(sourceTypeName, length, scale);
                break;
            default:
                String errMsg = "Not support " + sourceTypeName + " schema change.";
                throw new UnsupportedOperationException(errMsg);
        }
        return dorisTypeName;
    }

    private boolean checkSchemaChange(String database, String table, SchemaChangeHelper.DDLSchema ddlSchema) throws IOException, IllegalArgumentException {
        Map<String, Object> param = SchemaChangeManager.buildRequestParam(ddlSchema.isDropColumn(), ddlSchema.getColumnName());
        return schemaChangeManager.checkSchemaChange(database, table, param);
    }

    private JsonNode extractJsonNode(JsonNode record, String key) {
        if (record != null && record.has(key)) {
            return record.get(key);
        }
        return record;
    }

    private String extractString(JsonNode record, String key) {
        return record != null && record.get(key) != null &&
                !(record.get(key) instanceof NullNode) ? record.get(key).asText() : null;
    }

    private Map<String, Object> extractDataRow(JsonNode record) {
        JsonNode recordData = record.get("data");
        Map<String, Object> recordMap = objectMapper.convertValue(recordData, new TypeReference<Map<String, Object>>() {
        });
        return recordMap != null ? recordMap : new HashMap<>();
    }

}
