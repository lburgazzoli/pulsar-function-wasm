package com.github.lburgazzoli.pulsar.function.wasm

import com.github.lburgazzoli.pulsar.function.wasm.support.PulsarTestClient
import com.github.lburgazzoli.pulsar.function.wasm.support.PulsarTestSpec
import groovy.util.logging.Slf4j
import org.apache.pulsar.client.api.Schema
import org.slf4j.LoggerFactory
import org.testcontainers.containers.PulsarContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.spock.Testcontainers
import org.testcontainers.utility.DockerImageName
import spock.lang.Shared
import spock.lang.Timeout

import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

@Slf4j
@Testcontainers
class WasmFunctionTest extends PulsarTestSpec {
    private static final String IMAGE_NAME = 'apachepulsar/pulsar:3.1.0'
    private static final DockerImageName IMAGE = DockerImageName.parse(IMAGE_NAME)

    @Shared
    PulsarContainer PULSAR = new PulsarContainer(IMAGE)
            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger('pulsar.container')))


    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    def 'to_upper'() {
        given:
            def data = 'foo'

            def r = runner(
                    PULSAR.pulsarBrokerUrl,
                    'src/test/resources/functions.wasm',
                    'to_upper',
                    [ 'sensors' ],
                    'output-1')

            r.start(false)

            def c = new PulsarTestClient<>(Schema.BYTES, PULSAR.pulsarBrokerUrl)

        when:
            c.send("sensors", data.getBytes(StandardCharsets.UTF_8))
        then:
            def msg = c.read('output-1')
            msg.value == data.toUpperCase(Locale.US).getBytes(StandardCharsets.UTF_8)

        cleanup:
            closeQuietly(c)
            closeQuietly(r)
    }

    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    def 'cbr'(String value, String topic) {
        given:
            def r = runner(
                    PULSAR.pulsarBrokerUrl,
                    'src/test/resources/functions.wasm',
                    'cbr',
                    [ 'sensors' ],
                    'output-1')

            r.start(false)

            def c = new PulsarTestClient<>(Schema.BYTES, PULSAR.pulsarBrokerUrl)
        when:
            c.send("sensors", value.getBytes(StandardCharsets.UTF_8))
        then:
            def msg = c.read(topic)
            msg.value == value.getBytes(StandardCharsets.UTF_8)

        cleanup:
            closeQuietly(c)
            closeQuietly(r)
        where:
            value  | topic
            "foo"  | "output-foo"
            "bar"  | "output-bar"
    }
}
