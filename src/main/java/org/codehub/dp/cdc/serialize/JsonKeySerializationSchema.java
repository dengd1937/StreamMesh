package org.codehub.dp.cdc.serialize;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONPath;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class JsonKeySerializationSchema implements SerializationSchema<String> {
    private static final Logger LOG = LoggerFactory.getLogger(JsonKeySerializationSchema.class);

    private final List<String> fieldPaths;

    public JsonKeySerializationSchema(List<String> fieldPaths) {
        this.fieldPaths = fieldPaths;
    }

    @Override
    public byte[] serialize(String element) {
        try {
            validateJson(element);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        StringBuilder sb = new StringBuilder();
        for (String path: fieldPaths) {
            String value = String.valueOf(JSONPath.eval(element, path));
            sb.append(value);
        }
        String key = sb.toString();
        LOG.info("Send data to Kafka with Key: " + key);
        return key.getBytes(StandardCharsets.UTF_8);
    }

    private void validateJson(String element) throws Exception {
        try {
            JSON.parse(element);
        } catch (Exception e) {
            throw new Exception("Serialized data is not in JSON format");
        }
    }
}
