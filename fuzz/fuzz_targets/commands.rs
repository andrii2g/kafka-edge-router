#![no_main]

use libfuzzer_sys::fuzz_target;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
enum Command {
    Subscribe {
        subscription_id: String,
        filter: serde_json::Value,
    },
    Unsubscribe {
        subscription_id: String,
    },
    Ping {
        opaque: Option<String>,
    },
}

fuzz_target!(|data: &[u8]| {
    if let Ok(command) = serde_json::from_slice::<Command>(data) {
        match command {
            Command::Subscribe {
                subscription_id,
                filter,
            } => {
                let _ = (subscription_id, filter);
            }
            Command::Unsubscribe { subscription_id } => {
                let _ = subscription_id;
            }
            Command::Ping { opaque } => {
                let _ = opaque;
            }
        }
    }
});
