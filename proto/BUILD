load("@rules_proto//proto:defs.bzl", "proto_library")
load("@rules_jvm_external//:defs.bzl", "artifact")

java_library(
    name = "proto",
    visibility = [
        "//:__subpackages__",
    ],
    exports = [
        ":kafka",
    ],
    runtime_deps = [
        artifact("com.google.protobuf:protobuf-java"),
    ],
)

java_proto_library(
    name = "kafka",
    deps = [":kafka_proto"],
)

proto_library(
    name = "kafka_proto",
    srcs = [
        ":keyvalue.proto",
        ":batch.proto",
    ],
    deps = [
        "@com_google_protobuf//:duration_proto",
    ],
)
