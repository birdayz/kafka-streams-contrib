load("@rules_java//java:defs.bzl", "java_library")
load("@rules_jvm_external//:defs.bzl", "java_export")

package(default_visibility = ["//visibility:public"])

java_library(
    name = "library",
    srcs = glob(
        ["src/main/java/**/*.java", "examples/*.java"],
    ),
    resources = glob(["src/main/resources/**"]),
    visibility = [
        "//visibility:public",
    ],
    deps = [
      "@maven//:com_google_protobuf_protobuf_java",
      "@maven//:org_apache_kafka_kafka_streams",
      "@maven//:org_apache_kafka_kafka_clients",
      "@maven//:org_slf4j_slf4j_api",
      "@maven//:com_google_guava_guava",
      "//proto",
    ],
)

java_test(
    name = "AllTests",
    test_class = "de.nerden.kafka.streams.processor.AllTest",
    srcs = glob(["src/test/java/**/*.java"]),
    deps = [
      "@maven//:org_apache_kafka_kafka_streams_test_utils",
      "@maven//:com_google_truth_truth",
      ":library",
      "@maven//:com_google_protobuf_protobuf_java",
      "@maven//:org_apache_kafka_kafka_streams",
      "@maven//:org_apache_kafka_kafka_clients",
      "@maven//:org_slf4j_slf4j_api",
      "@maven//:com_google_guava_guava",
      "//proto",
    ],

)


java_export(
  name = "library-export",
  maven_coordinates = "de.nerden:kafka-streams-contrib:0.3.0",
  runtime_deps = [
    "//:library",
  ],
)

java_binary(
    name = "async-processor-example",
    main_class = "de.nerden.kafka.streams.processor.examples.AsyncProcessorExample",
    runtime_deps = [
      ":library",
      "@maven//:org_slf4j_slf4j_simple",
    ]
)

java_binary(
    name = "batch-processor-example",
    main_class = "de.nerden.kafka.streams.processor.examples.BatchingProcessorExample",
    runtime_deps = [
      ":library",
      "@maven//:org_slf4j_slf4j_simple",
    ]
)

