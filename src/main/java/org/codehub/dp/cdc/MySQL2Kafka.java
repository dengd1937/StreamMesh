package org.codehub.dp.cdc;

import com.google.common.collect.Lists;
import org.codehub.dp.context.StreamEnvContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MySQL2Kafka {

    private static final String KAFKA_KEY_FIELD_PATH = "$.pk";

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8020);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.enableCheckpointing(5000);
        env.setParallelism(1);

        String paramsFilePath = args[0];

        DataStream<String> cdcSource = StreamEnvContext.getMysqlCdcSource(env, paramsFilePath);
        Sink<String> kafkaSink = StreamEnvContext.getKafkaSink(paramsFilePath, Lists.newArrayList(KAFKA_KEY_FIELD_PATH));
        cdcSource.sinkTo(kafkaSink);

        StreamEnvContext.jobExecute(env);
    }

}
