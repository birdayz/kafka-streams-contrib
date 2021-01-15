load("@rules_java//java:defs.bzl", "java_library")
load("@rules_jvm_external//:defs.bzl", "java_export")
load("//:junit5.bzl", "java_junit5_test")

package(default_visibility = ["//visibility:public"])

java_library(
    name = "library",
    srcs = glob(
        [
            "src/main/java/**/*.java",
            "examples/*.java",
        ],
    ),
    resources = glob(["src/main/resources/**"]),
    visibility = [
        "//visibility:public",
    ],
    deps = [
        "//proto",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:org_apache_kafka_kafka_clients",
        "@maven//:org_apache_kafka_kafka_streams",
        "@maven//:org_slf4j_slf4j_api",
    ],
)

java_junit5_test(
    name = "all-tests",
    srcs = glob(["src/test/java/**/*.java"]),
    args = [
        "--select-package",
        "de.nerden.kafka.streams",
    ],
    main_class = "de.nerden.kafka.streams.BazelJUnit5ConsoleLauncher",
    test_package = "de.nerden.kafka.streams",
    use_testrunner = False,
    runtime_deps = [
        "@maven//:org_junit_platform_junit_platform_commons",
        "@maven//:org_junit_platform_junit_platform_console",
        "@maven//:org_junit_platform_junit_platform_launcher",
        "@maven//:org_junit_platform_junit_platform_suite_api",
        "@maven//:org_slf4j_slf4j_simple",
    ],
    deps = [
        ":library",
        "//proto",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_google_truth_truth",
        "@maven//:org_apache_kafka_kafka_clients",
        "@maven//:org_apache_kafka_kafka_streams",
        "@maven//:org_apache_kafka_kafka_streams_test_utils",
        "@maven//:org_junit_platform_junit_platform_commons",
        "@maven//:org_junit_platform_junit_platform_console",
        "@maven//:org_junit_platform_junit_platform_launcher",
        "@maven//:org_junit_platform_junit_platform_suite_api",
        "@maven//:org_slf4j_slf4j_api",
    ],
)

java_export(
    name = "library-export",
    maven_coordinates = "de.nerden:kafka-streams-contrib:VERSION",
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
    ],
)

java_binary(
    name = "batch-processor-example",
    main_class = "de.nerden.kafka.streams.processor.examples.BatchingProcessorExample",
    runtime_deps = [
        ":library",
        "@maven//:org_slf4j_slf4j_simple",
    ],
)
