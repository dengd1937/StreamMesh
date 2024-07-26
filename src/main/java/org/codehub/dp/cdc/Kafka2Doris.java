package org.codehub.dp.cdc;

import com.alibaba.fastjson.JSONObject;
import org.codehub.dp.context.StreamEnvContext;
import org.codehub.dp.util.JdbcUtil;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.codehub.dp.context.StreamSinkConfig.*;

public class Kafka2Doris {

    private static final Logger log = LoggerFactory.getLogger(Kafka2Doris.class);

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8030);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.enableCheckpointing(5000);
        env.setParallelism(1);

        String paramsFilePath = args[0];
        Map<String, String> tableColumnsMap = getSyncTables(paramsFilePath);
        Map<String, OutputTag<String>> outputTagHashMap = new HashMap<>();
        for (String t: tableColumnsMap.keySet()) {
            outputTagHashMap.put(t, new OutputTag<String>(t){});
        }

        DataStreamSource<String> kafkaSource = StreamEnvContext.getKafkaSource(env, paramsFilePath);
        SingleOutputStreamOperator<String> process = kafkaSource.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String input, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                JSONObject inputObj = JSONObject.parseObject(input);
                String table = inputObj.getString("table");
                if (outputTagHashMap.containsKey(table)) {
                    ctx.output(outputTagHashMap.get(table), input);
                } else {
                    log.warn("filter other table:{}", table);
                }
            }
        });

        for (Map.Entry<String, OutputTag<String>> entry: outputTagHashMap.entrySet()) {
            String table = entry.getKey();
            Sink<String> dorisSink = StreamEnvContext.buildDorisSink(paramsFilePath, table);
            process.getSideOutput(entry.getValue()).sinkTo(dorisSink).name(table);
        }

        StreamEnvContext.jobExecute(env);
    }

    private static Map<String, String> getSyncTables(String paramsPath) throws Exception {
        ParameterTool params = ParameterTool.fromPropertiesFile(paramsPath);
        String host = params.getRequired(DORIS_SINK_REQUIRED_HOST);
        int port = Integer.parseInt(params.get(DORIS_SINK_OPTIONAL_JDBC_PORT, "9030"));
        String username = params.getRequired(DORIS_SINK_REQUIRED_USERNAME);
        String password = params.getRequired(DORIS_SINK_REQUIRED_PASSWORD);
        String database = params.getRequired(DORIS_SINK_REQUIRED_DATABASE);
        List<String> tables = JdbcUtil.getDorisTables(database, host, port, username, password);

        String syncTables = params.get(DORIS_SINK_OPTIONAL_SYNC_TABLES, null);
        Pattern syncPattern = syncTables == null ? null : Pattern.compile(syncTables);
        HashMap<String, String> tableColumnsMap = new HashMap<>();
        for (String table: tables) {
            if (syncPattern != null && !syncPattern.matcher(table).matches()) {
                continue;
            }
            String columns = JdbcUtil.getDorisTableColumns(database, table, host, port, username, password);
            tableColumnsMap.put(table, columns);
        }
        return tableColumnsMap;
    }
}
