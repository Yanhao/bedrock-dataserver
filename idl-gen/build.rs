fn main() {
    prost_build::compile_protos(
        &[
            "../proto/message.proto",
            "../proto/wal.proto",
            "../proto/replog.proto",
        ],
        &["../"],
    )
    .unwrap();

    tonic_build::compile_protos("../proto/dataserver.proto").unwrap();

    tonic_build::compile_protos("../proto/metaserver.proto").unwrap();

    // tonic_build::compile_protos("src/proto/replog.proto").unwrap();
}
