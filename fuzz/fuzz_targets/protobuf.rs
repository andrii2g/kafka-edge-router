#![no_main]

use libfuzzer_sys::fuzz_target;
use prost::Message;

fuzz_target!(|data: &[u8]| {
    let _ = router_proto::v1::ClientCommand::decode(data);
    let _ = router_proto::v1::PublishRequest::decode(data);
});
