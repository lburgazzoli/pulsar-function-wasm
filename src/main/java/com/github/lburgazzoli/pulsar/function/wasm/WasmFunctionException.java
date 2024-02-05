package com.github.lburgazzoli.pulsar.function.wasm;

public class WasmFunctionException extends Exception {
    private final String functionName;

    public WasmFunctionException(String functionName, String message) {
        super(message);

        this.functionName = functionName;
    }

    public WasmFunctionException(String functionName, String message, Throwable cause) {
        super(message, cause);

        this.functionName = functionName;
    }

    public WasmFunctionException(String functionName, Throwable cause) {
        super(cause);

        this.functionName = functionName;
    }

    public String getFunctionName() {
        return functionName;
    }
}
