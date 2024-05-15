package com.github.lburgazzoli.pulsar.function.wasm;

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
import com.dylibso.chicory.runtime.WasmFunctionHandle;
import com.dylibso.chicory.runtime.exceptions.WASMMachineException;
import com.dylibso.chicory.wasm.types.Value;
import com.dylibso.chicory.wasm.types.ValueType;

public class WasmRecordProcessor implements AutoCloseable, Function<Context, Record<byte[]>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(WasmFunction.class);

    public static final String MODULE_NAME = "env";
    public static final String FN_ALLOC = "alloc";
    public static final String FN_DEALLOC = "dealloc";

    private final String functionName;

    private final Instance instance;
    private final ExportFunction function;
    private final ExportFunction alloc;
    private final ExportFunction dealloc;

    private final Object lock;
    private final AtomicReference<WasmRecord> ref;

    public WasmRecordProcessor(
        Module module,
        String functionName) {

        Objects.requireNonNull(module);
        Objects.requireNonNull(functionName);

        this.lock = new Object();
        this.ref = new AtomicReference<>();

        this.functionName = Objects.requireNonNull(functionName);
        this.instance = module.withHostImports(imports()).instantiate();
        this.function = this.instance.export(this.functionName);
        this.alloc = this.instance.export(FN_ALLOC);
        this.dealloc = this.instance.export(FN_DEALLOC);
    }

    @Override
    public Record<byte[]> apply(Context context) {
        //
        // TODO: check if this is really needed i.e. on Kafka Connect the
        //       transformation pipeline is single threaded
        //
        synchronized (lock) {
            try {
                ref.set(new WasmRecord(context));

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
                wrap(
                    this::getPropertyFn,
                    "pulsar_get_property",
                    List.of(ValueType.I32, ValueType.I32),
                    List.of(ValueType.I64)),
                wrap(
                    this::setPropertyFn,
                    "pulsar_set_property",
                    List.of(ValueType.I32, ValueType.I32, ValueType.I32, ValueType.I32),
                    List.of()),
                wrap(
                    this::removePropertyFn,
                    "pulsar_remove_property",
                    List.of(ValueType.I32, ValueType.I32),
                    List.of()),
                wrap(
                    this::getKeyFn,
                    "pulsar_get_key",
                    List.of(),
                    List.of(ValueType.I64)),
                wrap(
                    this::setKeyFn,
                    "pulsar_set_key",
                    List.of(ValueType.I32, ValueType.I32),
                    List.of()),
                wrap(
                    this::getValueFn,
                    "pulsar_get_value",
                    List.of(),
                    List.of(ValueType.I64)),
                wrap(
                    this::setValueFn,
                    "pulsar_set_value",
                    List.of(ValueType.I32, ValueType.I32),
                    List.of()),
                wrap(
                    this::getRecordTopicFn,
                    "pulsar_get_record_topic",
                    List.of(),
                    List.of(ValueType.I64)),
                wrap(
                    this::setRecordTopicFn,
                    "pulsar_set_record_topic",
                    List.of(ValueType.I32, ValueType.I32),
                    List.of()),
                wrap(
                    this::getDestinationTopicNameFn,
                    "pulsar_get_destination_topic",
                    List.of(),
                    List.of(ValueType.I64)),
                wrap(
                    this::setDestinationTopicNameFn,
                    "pulsar_set_destination_topic",
                    List.of(ValueType.I32, ValueType.I32),
                    List.of()),
                wrap(
                    this::getSchemaFn,
                    "pulsar_get_schema",
                    List.of(),
                    List.of(ValueType.I64)),
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

    /**
     * Wrap a {@link WasmFunctionHandle} so that we can add some per function metric.
     *
     * @param  handle      the {@link WasmFunctionHandle} to be executed
     * @param  name        the name fo the host function
     * @param  paramTypes  the list of parameters types expected by the function
     * @param  returnTypes the list of return types produces by the functions
     * @return             an instance of {@link HostFunction}
     */
    private HostFunction wrap(
        WasmFunctionHandle handle,
        String name,
        List<ValueType> paramTypes,
        List<ValueType> returnTypes) {

        final String invocations = String.format("wasm.%s.%s.invocations", MODULE_NAME, name);
        final String failures = String.format("wasm.%s.%s.failures", MODULE_NAME, name);

        return new HostFunction(
            (Instance instance, Value... args) -> {
                WasmRecord record = ref.get();
                try {
                    record.context().recordMetric(invocations, 1);
                    return handle.apply(instance, args);
                } catch (Exception e) {
                    record.context().recordMetric(failures, 1);
                    throw e;
                }
            },
            MODULE_NAME,
            name,
            paramTypes,
            returnTypes);
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
        byte[] data = propertyData != null ? WasmSupport.bytes(propertyData) : WasmSupport.emptyByteArray();

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

        this.ref.get().property(propertyName, WasmSupport.string(propertyData));

        return new Value[] {};
    }

    private Value[] removePropertyFn(Instance instance, Value... args) {
        final int addr = args[0].asInt();
        final int size = args[1].asInt();
        final String propertyName = instance.memory().readString(addr, size);

        this.ref.get().removeProperty(propertyName);

        return new Value[] {};
    }

    //
    // Key
    //

    private Value[] getKeyFn(Instance instance, Value... args) {
        final byte[] rawData = this.ref.get().key()
            .map(WasmSupport::bytes)
            .orElseGet(WasmSupport::emptyByteArray);

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
    // Schema
    //

    private Value[] getSchemaFn(Instance instance, Value... args) {
        final byte[] rawData = this.ref.get().schema().getSchemaInfo().getSchema();

        return new Value[] {
                write(rawData)
        };
    }

    //
    // Record Topic
    //

    private Value[] getRecordTopicFn(Instance instance, Value... args) {
        byte[] rawData = this.ref.get()
            .recordTopic().map(WasmSupport::bytes)
            .orElseGet(WasmSupport::emptyByteArray);

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
        byte[] rawData = WasmSupport.bytes(this.ref.get().destinationTopic());

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
