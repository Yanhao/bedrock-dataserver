fn main() {
    prost_build::compile_protos(
        &[
            "src/proto/message.proto",
            "src/proto/wal.proto"],
        &["src/"]).unwrap();

    tonic_build::compile_protos(
        &["src/proto/service.proto"]
    ).unwrap();
}
