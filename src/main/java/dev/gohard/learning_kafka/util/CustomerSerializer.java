package dev.gohard.learning_kafka.util;

import java.nio.ByteBuffer;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import dev.gohard.learning_kafka.Customer;

public class CustomerSerializer implements Serializer<Customer> {

    @Override
    public byte[] serialize(String topic, Customer data) {
        try {
            byte[] serializedName;
            int stringSize;

            if (data == null) return null;
            else {
                if (data.getName() != null) {
                    serializedName = data.getName().getBytes("UTF-8");
                    stringSize = serializedName.length;
                } else {
                    serializedName = new byte[0];
                    stringSize = 0;
                }

                ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + stringSize);
                buffer.putInt(data.getId());
                buffer.putInt(stringSize);
                buffer.put(serializedName);

                return buffer.array();
            }

        } catch (Exception e) {
            throw new SerializationException("Error when serializing Customer to byte[] " + e);      
        }
    }
}
