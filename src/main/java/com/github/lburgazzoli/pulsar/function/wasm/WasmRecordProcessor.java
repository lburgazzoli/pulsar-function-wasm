package com.github.lburgazzoli.pulsar.function.wasm;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dylibso.chicory.runtime.ExportFunction;
import com.dylibso.chicory.runtime.HostFunction;
import com.dylibso.chicory.runtime.HostImports;
import com.dylibso.chicory.runtime.Instance;
import com.dylibso.chicory.runtime.Module;
import com.dylibso.chicory.runtime.exceptions.WASMMachineException;
import com.dylibso.chicory.wasm.types.Value;
import com.dylibso.chicory.wasm.types.ValueType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

public class WasmRecordProcessor implements AutoCloseable, Function<Context, Record<byte[]>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(WasmFunction.class);

    public static final ObjectMapper MAPPER = JsonMapper.builder().build();

    public static final String MODULE_NAME = "env";
    public static final String FN_ALLOC = "alloc";
    public static final String FN_DEALLOC = "dealloc";

    private final Module module;
    private final String functionName;

    private final Instance instance;
    private final ExportFunction function;
    private final ExportFunction alloc;
    private final ExportFunction dealloc;

    private final AtomicReference<WasmRecord> ref;

    public WasmRecordProcessor(
        Module module,
        String functionName) {

        this.ref = new AtomicReference<>();
        this.module = Objects.requireNonNull(module);
        this.functionName = Objects.requireNonNull(functionName);
        this.instance = this.module.instantiate(imports());
        this.function = this.instance.export(this.functionName);
        this.alloc = this.instance.export(FN_ALLOC);
        this.dealloc = this.instance.export(FN_DEALLOC);
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public Record<byte[]> apply(Context context) {
        try {
            WasmRecord ctx = new WasmRecord(context);

            ref.set(ctx);

            Value[] results = function.apply();

            if (results != null) {
                int outAddr = -1;
                int outSize = 0;

                try {
                    long ptrAndSize = results[0].asLong();

                    outAddr = (int) (ptrAndSize >> 32);
                    outSize = (int) ptrAndSize;

                    // assume the max output is 31 bit, leverage the first bit for
                    // error detection
                    if (isError(outSize)) {
                        int errSize = errSize(outSize);
                        String errData = instance.memory().readString(outAddr, errSize);

                        throw new WasmFunctionException(this.functionName, errData);
                    }
                } finally {
                    if (outAddr != -1) {
                        dealloc.apply(Value.i32(outAddr), Value.i32(outSize));
                    }
                }
            }

            return ref.get().record();
        } catch (WASMMachineException e) {
            LOGGER.warn("message: {}, stack {}", e.getMessage(), e.stackFrames());
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            ref.set(null);
        }
    }

    @Override
    public void close() throws Exception {
    }

    private static boolean isError(int number) {
        return (number & (1 << 31)) != 0;
    }

    private static int errSize(int number) {
        return number & (~(1 << 31));
    }

    private HostImports imports() {
        HostFunction[] functions = new HostFunction[] {
                new HostFunction(
                    this::getPropertyFn,
                    MODULE_NAME,
                    "pulsar_get_property",
                    List.of(ValueType.I32, ValueType.I32),
                    List.of(ValueType.I64)),
                new HostFunction(
                    this::setPropertyFn,
                    MODULE_NAME,
                    "pulsar_set_property",
                    List.of(ValueType.I32, ValueType.I32, ValueType.I32, ValueType.I32),
                    List.of()),
                new HostFunction(
                    this::getKeyFn,
                    MODULE_NAME,
                    "pulsar_get_key",
                    List.of(),
                    List.of(ValueType.I64)),
                new HostFunction(
                    this::setKeyFn,
                    MODULE_NAME,
                    "pulsar_set_key",
                    List.of(ValueType.I32, ValueType.I32),
                    List.of()),
                new HostFunction(
                    this::getValueFn,
                    MODULE_NAME,
                    "pulsar_get_value",
                    List.of(),
                    List.of(ValueType.I64)),
                new HostFunction(
                    this::setValueFn,
                    MODULE_NAME,
                    "pulsar_set_value",
                    List.of(ValueType.I32, ValueType.I32),
                    List.of()),
                new HostFunction(
                    this::getRecordTopicFn,
                    MODULE_NAME,
                    "pulsar_get_record_topic",
                    List.of(),
                    List.of(ValueType.I64)),
                new HostFunction(
                    this::setRecordTopicFn,
                    MODULE_NAME,
                    "pulsar_set_record_topic",
                    List.of(ValueType.I32, ValueType.I32),
                    List.of()),
                new HostFunction(
                    this::getDestinationTopicNameFn,
                    MODULE_NAME,
                    "pulsar_get_destination_topic",
                    List.of(),
                    List.of(ValueType.I64)),
                new HostFunction(
                    this::setDestinationTopicNameFn,
                    MODULE_NAME,
                    "pulsar_set_destination_topic",
                    List.of(ValueType.I32, ValueType.I32),
                    List.of())
        };

        return new HostImports(functions);
    }

    /**
     * Write the give data to Wasm's linear memory.
     *
     * @param  data the data to be written
     * @return      an i64 holding the address and size fo the written data
     */
    private Value write(byte[] data) {
        int rawDataAddr = alloc.apply(Value.i32(data.length))[0].asInt();

        instance.memory().write(rawDataAddr, data);

        long ptrAndSize = rawDataAddr;
        ptrAndSize = ptrAndSize << 32;
        ptrAndSize = ptrAndSize | data.length;

        return Value.i64(ptrAndSize);

    }

    //
    // Functions
    //
    // Memory must be de-allocated by the Wasm Module
    //

    //
    // Properties
    //

    private Value[] getPropertyFn(Instance instance, Value... args) {
        final int addr = args[0].asInt();
        final int size = args[1].asInt();

        final String propertyName = instance.memory().readString(addr, size);
        final String propertyData = this.ref.get().property(propertyName);
        byte[] data = propertyData != null ? propertyData.getBytes(StandardCharsets.UTF_8) : new byte[] {};

        return new Value[] {
                write(data)
        };
    }

    private Value[] setPropertyFn(Instance instance, Value... args) {
        final int headerNameAddr = args[0].asInt();
        final int headerNameSize = args[1].asInt();
        final int headerDataAddr = args[2].asInt();
        final int headerDataSize = args[3].asInt();

        final String propertyName = instance.memory().readString(headerNameAddr, headerNameSize);
        final byte[] propertyData = instance.memory().readBytes(headerDataAddr, headerDataSize);

        this.ref.get().property(propertyName, new String(propertyData, StandardCharsets.UTF_8));

        return new Value[] {};
    }

    //
    // Key
    //

    private Value[] getKeyFn(Instance instance, Value... args) {
        final byte[] rawData = this.ref.get().key()
            .map(v -> v.getBytes(StandardCharsets.UTF_8))
            .orElseGet(() -> new byte[] {});

        return new Value[] {
                write(rawData)
        };
    }

    private Value[] setKeyFn(Instance instance, Value... args) {
        final int addr = args[0].asInt();
        final int size = args[1].asInt();
        final String key = instance.memory().readString(addr, size);

        this.ref.get().key(key);

        return new Value[] {};
    }

    //
    // Value
    //

    private Value[] getValueFn(Instance instance, Value... args) {
        final byte[] rawData = this.ref.get().value();

        return new Value[] {
                write(rawData)
        };
    }

    private Value[] setValueFn(Instance instance, Value... args) {
        final int addr = args[0].asInt();
        final int size = args[1].asInt();
        final byte[] value = instance.memory().readBytes(addr, size);

        this.ref.get().value(value);

        return new Value[] {};
    }

    //
    // Record Topic
    //

    private Value[] getRecordTopicFn(Instance instance, Value... args) {
        byte[] rawData = this.ref.get()
            .recordTopic().map(v -> v.getBytes(StandardCharsets.UTF_8))
            .orElseGet(() -> new byte[] {});

        return new Value[] {
                write(rawData)
        };
    }

    private Value[] setRecordTopicFn(Instance instance, Value... args) {
        final int addr = args[0].asInt();
        final int size = args[1].asInt();
        final String topic = instance.memory().readString(addr, size);

        this.ref.get().recordTopic(topic);

        return new Value[] {};
    }

    //
    // Destination Topic
    //

    private Value[] getDestinationTopicNameFn(Instance instance, Value... args) {
        byte[] rawData = this.ref.get().destinationTopic().getBytes(StandardCharsets.UTF_8);

        return new Value[] {
                write(rawData)
        };
    }

    private Value[] setDestinationTopicNameFn(Instance instance, Value... args) {
        final int addr = args[0].asInt();
        final int size = args[1].asInt();
        final String topic = instance.memory().readString(addr, size);

        this.ref.get().destinationTopic(topic);

        return new Value[] {};
    }
}
