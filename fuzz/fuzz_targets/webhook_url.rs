#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(input) = std::str::from_utf8(data) {
        if let Ok(url) =
            router_webhook::validate_destination_url(input, &[], false, false)
        {
            let _ = router_webhook::validate_destination_port(&url, &[]);
        }
    }
});
