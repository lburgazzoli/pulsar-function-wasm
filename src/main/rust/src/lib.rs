use std::mem;

#[cfg_attr(all(target_arch = "wasm32"), export_name = "alloc")]
#[no_mangle]
pub extern "C" fn alloc(size: u32) -> *mut u8 {
    let mut buf = Vec::with_capacity(size as usize);
    let ptr = buf.as_mut_ptr();

    // tell Rust not to clean this up
    mem::forget(buf);

    ptr
}

#[cfg_attr(all(target_arch = "wasm32"), export_name = "dealloc")]
#[no_mangle]
pub unsafe extern "C" fn dealloc(ptr: &mut u8, len: i32) {
    // Retakes the pointer which allows its memory to be freed.
    let _ = Vec::from_raw_parts(ptr, 0, len as usize);
}

// *****************************************************************************
//
// Host Functions and Helpers
//
// ******************************************************************************

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

pub fn get_record_value() -> Vec<u8> {
    let ptr_and_len = unsafe {
        pulsar_get_value()
    };

    let in_ptr = (ptr_and_len >> 32) as *mut u8;
    let in_len = (ptr_and_len as u32) as usize;

    return unsafe {
        Vec::from_raw_parts(in_ptr, in_len, in_len)
    };
}

pub fn set_record_value(v: Vec<u8>) {
     let out_len = v.len();
     let out_ptr = v.as_ptr();

     unsafe {
        pulsar_set_value(out_ptr, out_len as i32);
     };
}

pub fn get_record_key() -> Vec<u8> {
    let ptr_and_len = unsafe {
        pulsar_get_key()
    };

    let in_ptr = (ptr_and_len >> 32) as *mut u8;
    let in_len = (ptr_and_len as u32) as usize;

    return unsafe {
        Vec::from_raw_parts(in_ptr, in_len, in_len)
    };
}

pub fn set_record_key(v: Vec<u8>) {
     let out_len = v.len();
     let out_ptr = v.as_ptr();

     unsafe {
        pulsar_set_key(out_ptr, out_len as i32);
     };
}

pub fn get_record_property(name: String) -> Vec<u8> {
    let mut hn_data = name.into_bytes();
    let hn_len = hn_data.len();
    let hn_ptr = hn_data.as_mut_ptr();

    let ptr_and_len = unsafe {
        pulsar_get_property(hn_ptr, hn_len as i32)
    };

    let in_ptr = (ptr_and_len >> 32) as *mut u8;
    let in_len = (ptr_and_len as u32) as usize;

    return unsafe {
        Vec::from_raw_parts(in_ptr, in_len, in_len)
    };
}

pub fn set_record_property(name: String, val: Vec<u8>) {
    let mut hn_data = name.into_bytes();
    let hn_len = hn_data.len();
    let hn_ptr = hn_data.as_mut_ptr();

    let val_len = val.len();
    let val_ptr = val.as_ptr();

    unsafe {
        pulsar_set_property(hn_ptr, hn_len as i32, val_ptr, val_len as i32)
    };
}


pub fn remove_record_property(name: String) {
    let mut hn_data = name.into_bytes();
    let hn_len = hn_data.len();
    let hn_ptr = hn_data.as_mut_ptr();

    unsafe {
        pulsar_remove_property(hn_ptr, hn_len as i32)
    };
}

pub fn get_record_topic() -> Vec<u8> {
    let ptr_and_len = unsafe {
        pulsar_get_record_topic()
    };

    let in_ptr = (ptr_and_len >> 32) as *mut u8;
    let in_len = (ptr_and_len as u32) as usize;

    return unsafe {
        Vec::from_raw_parts(in_ptr, in_len, in_len)
    };
}

pub fn set_record_topic(v: Vec<u8>) {
    let out_len = v.len();
    let out_ptr = v.as_ptr();

    unsafe {
        pulsar_set_record_topic(out_ptr, out_len as i32);
    };
}


pub fn get_destination_topic() -> Vec<u8> {
    let ptr_and_len = unsafe {
        pulsar_get_destination_topic()
    };

    let in_ptr = (ptr_and_len >> 32) as *mut u8;
    let in_len = (ptr_and_len as u32) as usize;

    return unsafe {
        Vec::from_raw_parts(in_ptr, in_len, in_len)
    };
}

pub fn set_destination_topic(v: Vec<u8>) {
    let out_len = v.len();
    let out_ptr = v.as_ptr();

    unsafe {
        pulsar_set_destination_topic(out_ptr, out_len as i32);
    };
}
