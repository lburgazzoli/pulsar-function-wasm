#![allow(dead_code)]

pub use pulsar_function_wasm_sdk::*;

// *****************************************************************************
//
// Functions
//
// ******************************************************************************

#[cfg_attr(all(target_arch = "wasm32"), export_name = "to_upper")]
#[no_mangle]
pub extern fn to_upper() {
    let val = get_record_value();
    let res = String::from_utf8(val).unwrap().to_uppercase().as_bytes().to_vec();

    set_record_value(res);
}

#[cfg_attr(all(target_arch = "wasm32"), export_name = "value_to_key")]
#[no_mangle]
pub extern fn value_to_key() {
    let val = get_record_value();

    set_record_key(val);
}

#[cfg_attr(all(target_arch = "wasm32"), export_name = "header_to_key")]
#[no_mangle]
pub extern fn header_to_key() {
    let val = get_record_property("the-key".to_string());

    set_record_key(val);
}

#[cfg_attr(all(target_arch = "wasm32"), export_name = "copy_property")]
#[no_mangle]
pub extern fn copy_property() {
    let val = get_record_property("the-property-in".to_string());

    set_record_property("the-property-out".to_string(), val);
}


#[cfg_attr(all(target_arch = "wasm32"), export_name = "cbr")]
#[no_mangle]
pub extern fn cbr() {
    let val = get_record_value();
    let str = String::from_utf8(val).unwrap();
    let out = format!("output-{}", str);

    set_destination_topic(out.into());
}
