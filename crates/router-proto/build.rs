//! Generates Rust bindings from the router protobuf contract.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    tonic_prost_build::configure()
        .build_client(true)
        .build_transport(false)
        .build_server(true)
        .file_descriptor_set_path(
            std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("router_descriptor.bin"),
        )
        .compile_protos(&["proto/router/v1/router.proto"], &["proto"])?;

    println!("cargo:rerun-if-changed=proto/router/v1/router.proto");
    Ok(())
}
