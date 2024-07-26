package org.codehub.dp.example.datagen;

import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * 生成假数据，支持生成文件、传送到Kafka
 */
public class FakeGen {

    public static void saveToFile(int contextNum, String filePath) throws IOException {
        OutputStream bos = new BufferedOutputStream(Files.newOutputStream(Paths.get(filePath)));
        for (int i = 1; i <= contextNum; i++) {
            bos.write(CourseFake.toJson().getBytes());
            bos.write(System.lineSeparator().getBytes());
        }
        IOUtils.close(bos);
    }

    public static void sendToKafka(int contextNum, String address, String topic) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, address);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 1; i <= contextNum; i++) {
            producer.send(new ProducerRecord<>(topic, null, CourseFake.toJson()));
        }
        producer.close();
    }

    public static void main(String[] args) throws IOException {
        String server = "hadoop001:9092";
        String topic = "pwc";
        sendToKafka(1000, server, topic);
    }

}
