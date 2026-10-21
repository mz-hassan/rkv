fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().compile_protos(&["../../proto/rkv.proto"], &["../../proto"])?;
    println!("cargo:rerun-if-changed=../../proto/rkv.proto");
    Ok(())
}
