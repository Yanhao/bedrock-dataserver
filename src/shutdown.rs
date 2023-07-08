use std::sync::atomic;

#[allow(unused)]
pub struct ShutDownHandler {
    exiting: atomic::AtomicBool,
}

impl ShutDownHandler {}
