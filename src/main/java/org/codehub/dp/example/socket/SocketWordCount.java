package org.codehub.dp.example.socket;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SocketWordCount {

    public static void main(String[] args) throws Exception {
        // 外部传参数
        final String host;
        final int port;
        try {
            ParameterTool tool = ParameterTool.fromArgs(args);
            host = tool.get("host", "localhost");
            port = tool.getInt("port");
        } catch (Exception e) {
            System.err.println(
                    "No port specified. Please run 'SocketWordCount "
                    + "--host <host> --port <port>', where host(localhost by default) "
                    + "and port is the address of the text server"
            );
            return;
        }

        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream(host, port)
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, collector) -> {
                    for (String s : value.split(",")) {
                        collector.collect(new Tuple2<>(s, 1));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(x -> x.f0)
                .sum(1)
                .print();


        env.execute("SocketWordCount");
    }

}
