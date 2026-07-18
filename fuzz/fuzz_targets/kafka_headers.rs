#![no_main]

use libfuzzer_sys::fuzz_target;
use rdkafka::{
    message::{Header, OwnedHeaders, OwnedMessage},
    Timestamp,
};

fuzz_target!(|data: &[u8]| {
    let split = data.len() / 2;
    let headers = OwnedHeaders::new()
        .insert(Header {
            key: "x-message-id",
            value: Some(&data[..split]),
        })
        .insert(Header {
            key: "x-tenant-id",
            value: Some(&data[split..]),
        })
        .insert(Header {
            key: "x-content-type",
            value: Some(b"application/octet-stream"),
        });
    let record = OwnedMessage::new(
        Some(data.to_vec()),
        None,
        "fuzz".to_owned(),
        Timestamp::NotAvailable,
        0,
        0,
        Some(headers),
    );
    let _ = router_kafka::decode_message(&record, 1_048_576);
});
