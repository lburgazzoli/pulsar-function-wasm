package com.github.lburgazzoli.pulsar.function.wasm

import com.github.lburgazzoli.pulsar.function.wasm.support.PulsarContainer
import com.github.lburgazzoli.pulsar.function.wasm.support.PulsarTestSpec
import groovy.util.logging.Slf4j
import org.apache.pulsar.client.api.Schema
import org.testcontainers.spock.Testcontainers
import spock.lang.Shared
import spock.lang.Timeout

import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

@Slf4j
@Testcontainers
class WasmFunctionTest extends PulsarTestSpec {
    @Shared
    PulsarContainer PULSAR = new PulsarContainer()

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

            def c = PULSAR.client(Schema.BYTES)

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

            def c = PULSAR.client(Schema.BYTES)
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
