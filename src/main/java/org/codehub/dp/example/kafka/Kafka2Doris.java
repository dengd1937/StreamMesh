package org.codehub.dp.example.kafka;

import org.codehub.dp.context.StreamEnvContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Kafka2Doris {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<String> source = StreamEnvContext.getKafkaSource(env, args);
        source.print();

        StreamEnvContext.jobExecute(env);
    }

}
