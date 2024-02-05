package com.github.lburgazzoli.pulsar.function.wasm.support

import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.api.Producer
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.PulsarClientException
import org.apache.pulsar.client.api.Reader
import org.apache.pulsar.client.api.Schema

class PulsarTestClient<T> implements Closeable {
    private final PulsarClient client
    private final Schema<T> schema

    PulsarTestClient(Schema<T> schema, String broker) throws PulsarClientException {
        this.schema = schema

        this.client = PulsarClient.builder()
                .serviceUrl(broker)
                .build()
    }

    void send(String topic, T value) throws Exception {
        try (Producer<T> p = client.newProducer(schema).topic(topic).create()) {
            p.send(value)
        }
    }

    Message<T> read(String topic) throws Exception {
        try (Reader<T> r = client.newReader(schema).topic(topic).startMessageId(MessageId.latest).create()) {
            return r.readNext()
        }
    }

    @Override
    void close() throws Exception {
        this.client?.close()
    }
}
