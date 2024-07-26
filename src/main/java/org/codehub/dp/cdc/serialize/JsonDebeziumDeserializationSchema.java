package org.codehub.dp.cdc.serialize;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import io.debezium.data.Envelope;
import io.debezium.time.*;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.utils.TemporalConversions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.Serializable;
import java.text.ParseException;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class JsonDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {

    private final String DATE_PATTERN = "yyyy-MM-dd";
    private final String DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private final String DATETIME_INSTANT_PATTERN = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private final TimeZone UTC = TimeZone.getTimeZone("UTC");
    private final Pattern pattern= Pattern.compile("cdc.including.*.columns");

    private final JsonDebeziumDeserializationSchema.DeserializationRuntimeConverter runtimeConverter;
    private Map<String, List<String>> tableColumnsMap = new HashMap<>();

    public JsonDebeziumDeserializationSchema() {
        this(0, null);
    }

    public JsonDebeziumDeserializationSchema(int zoneOffset) {
        this(zoneOffset, null);
    }

    public JsonDebeziumDeserializationSchema(Configuration config) {
        this(0, config);
    }

    public JsonDebeziumDeserializationSchema(int zoneOffset, Configuration config) {

        //实现一个用于转换时间的Converter
        this.runtimeConverter = (dbzObj,schema) -> {
            if(schema.name() != null && dbzObj != null){
                switch (schema.name()) {
                    case Timestamp.SCHEMA_NAME:
                        return TimestampData.fromEpochMillis((Long) dbzObj)
                                .toLocalDateTime()
                                .plusHours(zoneOffset)
                                .format(DateTimeFormatter.ofPattern(DATETIME_PATTERN));
                    case ZonedTimestamp.SCHEMA_NAME:
                        FastDateFormat utcFormat = FastDateFormat.getInstance(DATETIME_INSTANT_PATTERN, UTC);
                        java.util.Date date = utcFormat.parse((String) dbzObj);
                        return FastDateFormat.getInstance(DATETIME_PATTERN).format(date);
                    case MicroTimestamp.SCHEMA_NAME:
                        long micro = (long) dbzObj;
                        return TimestampData.fromEpochMillis(micro / 1000, (int) (micro % 1000 * 1000))
                                .toLocalDateTime()
                                .plusHours(zoneOffset)
                                .format(DateTimeFormatter.ofPattern(DATETIME_PATTERN));
                    case NanoTimestamp.SCHEMA_NAME:
                        long nano = (long) dbzObj;
                        return TimestampData.fromEpochMillis(nano / 1000_000, (int) (nano % 1000_000))
                                .toLocalDateTime()
                                .plusHours(zoneOffset)
                                .format(DateTimeFormatter.ofPattern(DATETIME_PATTERN));
                    case Date.SCHEMA_NAME:
                        return TemporalConversions.toLocalDate(dbzObj).format(DateTimeFormatter.ofPattern(DATE_PATTERN));
                }
            }
            return dbzObj;
        };

        // 判断是否存在只取表的某些字段来同步
        config.keySet().forEach(k -> {
            if (pattern.matcher(k).matches()) {
                String tableName = k.split("\\.")[2];
                String columns = config.getString(k, null);
                List<String> columnsList = columns == null ? Lists.newArrayList() : Lists.newArrayList(columns.split(","));
                tableColumnsMap.put(tableName, columnsList);
            }
        });
    }

    private interface DeserializationRuntimeConverter extends Serializable {
        Object convert(Object dbzObj, Schema schema) throws ParseException;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<String> out) throws Exception {
        HashMap<String, Object> changes = new HashMap<>();

        Struct value = (Struct) record.value();
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        String table = source.getString("table");
        changes.put("table", table);
        changes.put("connector", source.getString("connector"));
        changes.put("db", source.getString("db"));
        if (source.schema().field("schema") != null) {
            changes.put("schema", source.getString("schema"));
        }
        Struct key = (Struct) record.key();
        changes.put("pk", getRowPk(key, table));

        if (value.schema().field("historyRecord") != null) {
            String historyRecord = value.getString("historyRecord");
            JSONObject historyRecordObj = JSONObject.parseObject(historyRecord);
            changes.put("historyRecord", JSONObject.toJavaObject(historyRecordObj, Map.class));
            changes.put("ts", source.getInt64("ts_ms"));
            out.collect(JSON.toJSONString(changes));
            return;
        }
        changes.put("ts", value.getInt64("ts_ms"));

        Envelope.Operation operation = Envelope.operationFor(record);
        changes.put("op", operation.code());
        if (operation == Envelope.Operation.CREATE || operation == Envelope.Operation.READ) {
            // insert
            changes.put("data", getRowMap(table, value.getStruct(Envelope.FieldName.AFTER)));
        } else if (operation == Envelope.Operation.UPDATE) {
            // update
            changes.put("data", getRowMap(table, value.getStruct(Envelope.FieldName.AFTER)));
        } else if (operation == Envelope.Operation.DELETE) {
            // delete
            changes.put("data", getRowMap(table, value.getStruct(Envelope.FieldName.BEFORE)));
        }

        out.collect(JSON.toJSONString(changes));
//        out.collect(record.toString());
    }

    private String getRowPk(Struct struct, String table) {
        if (struct == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder(table).append("_");
        struct.schema().fields().forEach(f -> sb.append(struct.get(f)).append("_"));
        return sb.toString();
    }

    private Map<String, String> getRowMap(String table, Struct struct) {
        Map<String, String> rowMap = new HashMap<>();
        List<String> filterColumnsList = tableColumnsMap.get(table);
        struct.schema().fields().forEach(f -> {
            String rowValue;
            String columnName = f.name();
            if (filterColumnsList != null && !filterColumnsList.contains(columnName)) {
                return;
            }
            try {
                rowValue = String.valueOf(runtimeConverter.convert(struct.get(f), f.schema()));
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            rowMap.put(columnName, rowValue);
        });
        return rowMap;
    }

    private Map<String, String> getRowSchema(Schema schema) {
        return schema.fields().stream().collect(Collectors.toMap(Field::name, f -> f.schema().type().getName()));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

}
