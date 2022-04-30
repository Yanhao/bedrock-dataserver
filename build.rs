fn main() {
    prost_build::compile_protos(
        &[
            "src/proto/message.proto",
            "src/proto/wal.proto",
            "src/proto/replog.proto",
        ],
        &["src/"],
    )
    .unwrap();

    tonic_build::compile_protos("src/proto/dataserver.proto").unwrap();

    tonic_build::compile_protos("src/proto/metaserver.proto").unwrap();

    // tonic_build::compile_protos("src/proto/replog.proto").unwrap();
}
