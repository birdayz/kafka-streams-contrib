load("@rules_java//java:defs.bzl", "java_library")
load("@rules_jvm_external//:defs.bzl", "java_export")

java_library(
    name = "kafka-streams-contrib",
    srcs = glob(
        ["src/main/java/**/*.java"],
    ),
    resources = glob(["src/main/resources/**"]),
    visibility = [
        "//:__pkg__",
    ],
    deps = [
      "@maven//:com_google_protobuf_protobuf_java",
      "@maven//:org_apache_kafka_kafka_streams",
      "@maven//:org_apache_kafka_kafka_clients",
      "@maven//:org_slf4j_slf4j_api",
      "@maven//:com_google_guava_guava",
      "//src/main/proto",
    ],
)

java_export(
  name = "kafka-streams-contrib-publish",
  maven_coordinates = "de.nerden:kafka-streams-contrib:0.1.1",
  runtime_deps = [
    "//:kafka-streams-contrib",
  ],
)

