extern crate prost_build;
fn main() {
    prost_build::compile_protos(&["./proto/v1/record.proto"], &[".proto/v1/"]).unwrap();
}
