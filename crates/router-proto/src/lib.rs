//! Generated public gRPC API.

/// Version 1 of the router service contract.
#[allow(
    missing_docs,
    clippy::all,
    clippy::pedantic,
    reason = "tonic and prost generate this module"
)]
pub mod v1 {
    tonic::include_proto!("router.v1");
}

/// Encoded protobuf descriptors used by the optional reflection service.
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("router_descriptor");
