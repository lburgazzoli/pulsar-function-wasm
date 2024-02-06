package com.github.lburgazzoli.pulsar.function.wasm;

import java.nio.charset.StandardCharsets;

public class WasmSupport {
    private static final byte[] EMPTY_ARRAY = new byte[0];

    private WasmSupport() {
    }

    public static byte[] emptyByteArray() {
        return EMPTY_ARRAY;
    }

    public static byte[] bytes(String in) {
        return in.getBytes(StandardCharsets.UTF_8);
    }

    public static String string(byte[] in) {
        return new String(in, StandardCharsets.UTF_8);
    }
}
