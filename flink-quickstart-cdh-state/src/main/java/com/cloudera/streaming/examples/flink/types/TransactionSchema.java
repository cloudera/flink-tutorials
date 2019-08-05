package com.cloudera.streaming.examples.flink.types;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class TransactionSchema implements KeyedSerializationSchema<ItemTransaction>, DeserializationSchema<ItemTransaction> {

    @Override
    public byte[] serializeKey(ItemTransaction t) {
        return t.itemId.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] serializeValue(ItemTransaction t) {
        return (t.transactionId + "\t" + t.ts + "\t" + t.itemId + "\t" + t.quantity).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String getTargetTopic(ItemTransaction t) {
        return null;
    }

    @Override
    public ItemTransaction deserialize(byte[] message) throws IOException {
        String[] split = new String(message, StandardCharsets.UTF_8).split("\t");
        return new ItemTransaction(Long.parseLong(split[0]), Long.parseLong(split[1]), split[2], Integer.parseInt(split[3]));
    }

    @Override
    public boolean isEndOfStream(ItemTransaction nextElement) {
        return false;
    }

    @Override
    public TypeInformation<ItemTransaction> getProducedType() {
        return new TypeHint<ItemTransaction>() {
        }.getTypeInfo();
    }
}
