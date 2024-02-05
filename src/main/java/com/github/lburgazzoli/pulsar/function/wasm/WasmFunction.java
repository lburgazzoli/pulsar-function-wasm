package com.github.lburgazzoli.pulsar.function.wasm;

import java.io.File;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Record;

import com.dylibso.chicory.runtime.Module;

public class WasmFunction implements org.apache.pulsar.functions.api.Function<byte[], Record<byte[]>> {
    public static final String KEY_MODULE = "module";
    public static final String KEY_FUNCTION = "function";

    private WasmRecordProcessor runner;

    public WasmFunction() {
    }

    @Override
    public void initialize(Context context) throws Exception {
        final String module = context.getUserConfigValue(WasmFunction.KEY_MODULE)
            .map(String.class::cast)
            .orElseThrow(() -> new IllegalArgumentException("Missing " + WasmFunction.KEY_MODULE + " config"));

        final String function = context.getUserConfigValue(WasmFunction.KEY_FUNCTION)
            .map(String.class::cast)
            .orElseThrow(() -> new IllegalArgumentException("Missing " + WasmFunction.KEY_FUNCTION + " config"));

        this.runner = new WasmRecordProcessor(
            Module.builder(new File(module)).build(),
            function);

    }

    @Override
    public void close() throws Exception {
        if (this.runner != null) {
            this.runner.close();
            this.runner = null;
        }
    }

    /*
     * TODO At this stage, we require to receive the serialized record value, but at some
     *      point it would be nice to receive the value as it is and then encode/decode ti
     *      on demand leveraging the associated schema.
     */
    @Override
    public Record<byte[]> process(byte[] input, Context context) throws Exception {
        context.getLogger().info(
            "apply: {} ({})",
            context.getCurrentRecord().getValue(),
            context.getCurrentRecord().getDestinationTopic());

        Record<byte[]> result = this.runner.apply(context);

        context.getLogger().info(
            "result: {} ({})",
            result.getValue(),
            result.getDestinationTopic());

        return result;
    }
}
