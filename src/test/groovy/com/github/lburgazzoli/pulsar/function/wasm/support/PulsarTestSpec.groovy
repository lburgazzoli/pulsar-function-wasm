package com.github.lburgazzoli.pulsar.function.wasm.support


import com.github.lburgazzoli.pulsar.function.wasm.WasmFunction
import org.apache.pulsar.common.functions.FunctionConfig
import org.apache.pulsar.functions.LocalRunner
import spock.lang.Specification

class PulsarTestSpec extends Specification {

    static LocalRunner runner(String url, String module, String function, Collection<String> inputs, String output) {
        FunctionConfig cfg = new FunctionConfig();
        cfg.setName('wasm')
        cfg.setClassName(WasmFunction.class.getName())
        cfg.setRuntime(FunctionConfig.Runtime.JAVA)
        cfg.setInputs(inputs)
        cfg.setOutput(output)
        cfg.setUserConfig(Map.of(
            WasmFunction.KEY_MODULE, module,
            WasmFunction.KEY_FUNCTION, function)
        )

        return LocalRunner.builder()
            .functionConfig(cfg)
            .brokerServiceUrl(url)
            .build()
    }

    static void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (final IOException|InterruptedException ignored) {
            }
        }
    }

}
