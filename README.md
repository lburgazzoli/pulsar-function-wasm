# pulsar-function-wasm

The Wasm Pulsar Function is a regular [Pulsar Function](https://pulsar.apache.org/docs/functions-overview/) that aims to provide support to leverage [WebAssembly (Wasm)](https://webassembly.org/) to process and transform data.


## Configuration

The `WasmFunction` reads its configuration from the Function `userConfig` `module` and `function` parameter, as example, a [function configuration file](https://pulsar.apache.org/docs/3.0.x/functions-cli/) could look like:

```yaml
tenant: "public"
namespace: "default"
name: "camel"
inputs:
  - "persistent://public/default/input-1"
output: "persistent://public/default/output-1"
jar: "build/libs/pulsar-function-wasm-${version}-all.jar"
className: "com.github.lburgazzoli.pulsar.function.wasm.WasmFunction"
logTopic: "persistent://public/default/logging-function-logs"
userConfig:
  module: 'src/test/resources/functions.wasm'
  function: 'to_upper'
```

## Function Implementation

The function register the following host functions to manipulate the incoming record:

```rust
extern "C" {
	fn pulsar_set_key(ptr: *const u8, len: i32);
	fn pulsar_get_key() -> u64;

	fn pulsar_set_value(ptr: *const u8, len: i32);
	fn pulsar_get_value() -> u64;

	fn pulsar_set_property(key_ptr: *const u8, lkey_len: i32, val_ptr: *const u8, val_len: i32);
	fn pulsar_get_property(ptr: *const u8, len: i32) -> u64;
	fn pulsar_remove_property(ptr: *const u8, len: i32);


	fn pulsar_set_record_topic(ptr: *const u8, len: i32);
	fn pulsar_get_record_topic() -> u64;

	fn pulsar_set_destination_topic(ptr: *const u8, len: i32);
	fn pulsar_get_destination_topic() -> u64;
}
```

Some helper functions are provided in the [lib.rs](src/main/rust/src/lib.rs)
Some example of function are provided in the [functions.rs](src/test/rust/functions.rs)

## Deployment

See [the Pulsar docs](https://pulsar.apache.org/fr/docs/functions-deploy) for more details on how to deploy a Function.


