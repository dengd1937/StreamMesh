package org.codehub.dp.example.cdc.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SingleTableSQLServer2KafkaTable {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8021);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.enableCheckpointing(5000);
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // source
        tEnv.executeSql("CREATE TABLE products (\n" +
                "    id INT,\n" +
                "    name STRING,\n" +
                "    description STRING,\n" +
                "    weight DOUBLE,\n" +
                "    create_time TIMESTAMP, \n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                "  ) WITH (\n" +
                "    'connector' = 'sqlserver-cdc',\n" +
                "    'hostname' = 'localhost',\n" +
                "    'port' = '1433',\n" +
                "    'username' = 'sa',\n" +
                "    'password' = '@root123456',\n" +
                "    'database-name' = 'inventory',\n" +
                "    'table-name' = 'dbo.products'\n" +
                "  )");

        // sink
        tEnv.executeSql("CREATE TABLE kafka_table (\n" +
                "  id INT,\n" +
                "  name STRING,\n" +
                "  description STRING,\n" +
                "  weight DOUBLE,\n" +
                "  create_time TIMESTAMP \n" +
                ") WITH (\n" +
                "  'connector' = 'kafka', \n" +
                "  'topic' = 'sqlserver_ddc_test_t_inventory_dbo_products_r1p1', \n" +
                "  'properties.bootstrap.servers' = 'localhost:9092', \n" +
                "  'properties.group.id' = 'testGroup', \n" +
                "  'key.format' = 'json', \n" +
                "  'value.format' = 'json' \n" +
                ")");
        tEnv.executeSql("INSERT INTO kafka_table SELECT * FROM products");
    }

}
