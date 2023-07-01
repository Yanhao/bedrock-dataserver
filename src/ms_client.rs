use once_cell::sync::Lazy;

pub static MS_CLIENT: Lazy<MsClient> = Lazy::new(|| MsClient::new());

pub struct MsClient {}

impl MsClient {
    pub fn new() -> Self {
        todo!()
    }
}
