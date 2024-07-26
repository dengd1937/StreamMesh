package org.codehub.dp.example.cdc;

import com.alibaba.fastjson.JSONObject;
import org.codehub.dp.cdc.serialize.JsonDebeziumDeserializationSchema;
import org.codehub.dp.context.StreamEnvContext;
import org.codehub.dp.util.DBType;
import org.codehub.dp.util.JdbcUtil;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.cdc.connectors.sqlserver.SqlServerSource;
import org.apache.flink.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SQLServerToDoris {

    private static final String SOURCE_DB = "inventory";//db
    private static final String SOURCE_IP = "localhost"; //hostname
    private static final String SOURCE_USER = "sa"; //username
    private static final String SOURCE_PWD = "@root123456"; //password
    private static final int SOURCE_PORT = 1433;


    private static final String DORIS_IP = "10.158.16.244";
    private static final String DORIS_PORT = "8030";
    private static final String DORIS_USER = "root";
    private static final String DORIS_PWD = "";
    private static final String DORIS_DB = "test";

    private static final Logger log = LoggerFactory.getLogger(SQLServerToDoris.class);

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8020);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.enableCheckpointing(5000);
        env.setParallelism(1);

        DebeziumSourceFunction<String> sqlserverSource = SqlServerSource.<String>builder()
                .hostname(SOURCE_IP)
                .port(SOURCE_PORT)
                .username(SOURCE_USER)
                .password(SOURCE_PWD)
                .database(SOURCE_DB)
                .deserializer(new JsonDebeziumDeserializationSchema(8))
                .build();

        HashMap<String, OutputTag<String>> outputTagHashMap = new HashMap<>();
        List<String> tableList = getTableList();
        if (tableList.size() == 0) {
            throw new RuntimeException("当前库中没有开启cdc的表");
        }
        for (String t: tableList) {
            outputTagHashMap.put(t, new OutputTag<String>(t){});
        }

        DataStreamSource<String> cdcSource = env.addSource(sqlserverSource);
        cdcSource.print();

        SingleOutputStreamOperator<String> process = cdcSource.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String input,
                                       ProcessFunction<String, String>.Context ctx,
                                       Collector<String> out) throws Exception {
                JSONObject inputObj = JSONObject.parseObject(input);
                String table = inputObj.getString("table");
                String op = inputObj.getString("op");
                JSONObject data = inputObj.getJSONObject("data");

                if (Arrays.asList("c", "r", "u").contains(op) && outputTagHashMap.containsKey(table)) {
                    data.put("__DORIS_DELETE_SIGN__", false);
                    ctx.output(outputTagHashMap.get(table), data.toJSONString());
                } else if ("d".equals(op) && outputTagHashMap.containsKey(table)) {
                    data.put("__DORIS_DELETE_SIGN__", true);
                    ctx.output(outputTagHashMap.get(table), data.toJSONString());
                } else {
                    log.warn("filter other op:{}, table{}", op, table);
                }

                log.warn("record: " + data.toJSONString());
            }
        });

        Map<String, String> tableColumn = getTableColumn();
        for (Map.Entry<String, OutputTag<String>> entry: outputTagHashMap.entrySet()) {
            String table = entry.getKey();
            String columns = tableColumn.get(table);
            process.getSideOutput(entry.getValue()).sinkTo(buildDorisSink(table, columns)).name(table);
        }

        StreamEnvContext.jobExecute(env);
    }

    // create doris sink
    public static DorisSink<String> buildDorisSink(String table, String tableColumn) {
        DorisSink.Builder<String> builder = DorisSink.builder();
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        String tbl = DORIS_DB + "." + table;
        dorisBuilder.setFenodes(DORIS_IP + ":" + DORIS_PORT)
                .setTableIdentifier(tbl)
                .setUsername(DORIS_USER)
                .setPassword(DORIS_PWD);

        Properties pro = new Properties();
        //json data format
        pro.setProperty("format", "json");
        pro.setProperty("read_json_by_line", "true");
        // 解决精度丢失
        pro.setProperty("num_as_string", "true");

        //delete use
        log.warn("tableColumn: " + tableColumn);
        pro.setProperty("sink.enable-delete", "true");
        pro.setProperty("columns", tableColumn + ",`__DORIS_DELETE_SIGN__`");
        DorisExecutionOptions executionOptions = DorisExecutionOptions.builder()
                .setLabelPrefix("label-" + UUID.randomUUID() + table) //streamload label prefix,
                .setStreamLoadProp(pro)
                .setDeletable(true)
                .build();
        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionOptions)
                .setSerializer(new SimpleStringSerializer()) //serialize according to string
                .setDorisOptions(dorisBuilder.build());

        return builder.build();
    }

    private static List<String> getTableList() throws Exception {
        List<String> list = new ArrayList<>();
        String sql = "SELECT NAME FROM " + SOURCE_DB + ".sys.tables WHERE is_tracked_by_cdc = 1";
        List<JSONObject> JSONObject = JdbcUtil.executeQuery(DBType.SQLSERVER, SOURCE_IP, SOURCE_PORT, SOURCE_USER, SOURCE_PWD, sql);
        if (null != JSONObject && JSONObject.size() > 0) {
            for (JSONObject json : JSONObject) {
                list.add(json.getString("NAME"));
            }
        }
        return list;
    }

    private static Map<String, String> getTableColumn() throws Exception {
        Map<String, String> reMap = new HashMap<>();
        String sql = "SELECT TABLE_SCHEMA, TABLE_NAME," +
                "stuff(" +
                "(SELECT ',' + '`' + t.COLUMN_NAME + '`' FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.COLUMNS t WHERE c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME FOR XML PATH('')" +
                "), 1, 1, '') as COLUMNS " +
                "FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.COLUMNS c WHERE TABLE_SCHEMA = 'dbo' GROUP BY TABLE_SCHEMA, TABLE_NAME";

        List<JSONObject> JSONObject = JdbcUtil.executeQuery(DBType.SQLSERVER, SOURCE_IP, SOURCE_PORT, SOURCE_USER, SOURCE_PWD, sql);
        if (null != JSONObject && JSONObject.size() > 0) {
            for (JSONObject json : JSONObject) {
                String tableName = json.getString("TABLE_NAME");
                String columnStr = json.getString("COLUMNS");
                reMap.put(tableName, columnStr);
            }
        }
        return reMap;
    }

}
