load("@rules_java//java:defs.bzl", "java_library")

java_library(
    name = "kafka-streams-contrib",
    srcs = glob(
        ["src/main/java/**/*.java"],
    ),
    resources = glob(["src/main/resources/**"]),
    deps = [
      "@maven//:com_google_protobuf_protobuf_java",
      "@maven//:org_apache_kafka_kafka_streams",
      "@maven//:org_slf4j_slf4j_api"
    ],
)

