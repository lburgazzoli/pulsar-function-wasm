package com.github.lburgazzoli.pulsar.function.wasm;

import java.util.Optional;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.utils.FunctionRecord;

public class WasmRecord {
    private final Context context;
    private Record<byte[]> record;

    @SuppressWarnings({ "unchecked" })
    public WasmRecord(Context context) {
        this.context = context;
        this.record = (Record<byte[]>) context.getCurrentRecord();
    }

    public Context context() {
        return context;
    }

    public Schema<byte[]> schema() {
        return record.getSchema();
    }

    public Record<byte[]> record() {
        return record;
    }

    public void key(String value) {
        this.record = builder().key(value).build();
    }

    public Optional<String> key() {
        return this.record.getKey();
    }

    public void recordTopic(String value) {
        this.record = builder().topicName(value).build();
    }

    public Optional<String> recordTopic() {
        return this.record.getTopicName();
    }

    public void destinationTopic(String value) {
        this.record = builder().destinationTopic(value).build();
    }

    public String destinationTopic() {
        return this.context.getOutputTopic();
    }

    public void property(String name, String value) {
        this.record.getProperties().put(name, value);
    }

    public String property(String name) {
        return this.record.getProperties().get(name);
    }

    public void removeProperty(String name) {
        this.record.getProperties().remove(name);
    }

    public void value(byte[] value) {
        this.record = builder().value(value).build();
    }

    public byte[] value() {
        return this.record.getValue();
    }

    private FunctionRecord.FunctionRecordBuilder<byte[]> builder() {
        FunctionRecord.FunctionRecordBuilder<byte[]> builder = this.context.newOutputRecordBuilder(record.getSchema());

        builder.value(record.getValue());
        builder.properties(record.getProperties());

        record.getKey().ifPresent(builder::key);
        record.getTopicName().ifPresent(builder::topicName);
        record.getPartitionId().ifPresent(builder::partitionId);
        record.getEventTime().ifPresent(builder::eventTime);
        record.getKey().ifPresent(builder::key);
        record.getDestinationTopic().ifPresent(builder::destinationTopic);

        return builder;
    }
}
