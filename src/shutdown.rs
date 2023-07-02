use std::sync::atomic;

pub struct ShutDownHandler {
    exiting: atomic::AtomicBool,
}

impl ShutDownHandler {}
