package com.github.lburgazzoli.pulsar.function.wasm.support

import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.api.Producer
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.PulsarClientException
import org.apache.pulsar.client.api.Reader
import org.apache.pulsar.client.api.Schema
import org.slf4j.LoggerFactory
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.utility.DockerImageName

class PulsarContainer extends org.testcontainers.containers.PulsarContainer {
    private static final String IMAGE_NAME = 'apachepulsar/pulsar:3.1.0'
    private static final DockerImageName IMAGE = DockerImageName.parse(IMAGE_NAME)

    PulsarContainer() {
        super(IMAGE)
    }

    @Override
    void close() {
        super.close()
    }

    @Override
    protected void configure() {
        super.configure()
        withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(PulsarContainer.class)))
    }

    <T> Client client(Schema<T> schema) {
        return new Client(schema, this.pulsarBrokerUrl)
    }

    static class Client<T> implements Closeable {
        private final PulsarClient client
        private final Schema<T> schema

        private Client(Schema<T> schema, String broker) throws PulsarClientException {
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

}



