load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
protobuf_version = "3.11.3"

RULES_JVM_EXTERNAL_TAG = "3.3"
RULES_JVM_EXTERNAL_SHA = "d85951a92c0908c80bd8551002d66cb23c3434409c814179c0ff026b53544dab"

http_archive(
    name = "rules_jvm_external",
    strip_prefix = "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
    sha256 = RULES_JVM_EXTERNAL_SHA,
    url = "https://github.com/bazelbuild/rules_jvm_external/archive/%s.zip" % RULES_JVM_EXTERNAL_TAG,
)

load("@rules_jvm_external//:defs.bzl", "maven_install")

maven_install(
    artifacts = [
      "com.google.protobuf:protobuf-java:3.13.0",
      "org.apache.kafka:kafka-streams:2.6.0",
      "org.apache.kafka:kafka-clients:2.6.0",
      "org.slf4j:slf4j-api:1.7.30",
      "com.google.guava:guava:29.0-jre"
    ],
    repositories = [
        "https://jcenter.bintray.com/",
        "https://repo1.maven.org/maven2",
    ],
)


http_archive(
    name = "com_google_protobuf",
    sha256 = "cf754718b0aa945b00550ed7962ddc167167bd922b842199eeb6505e6f344852",
    strip_prefix = "protobuf-%s" % protobuf_version,
    urls = [
        "https://mirror.bazel.build/github.com/protocolbuffers/protobuf/archive/v%s.tar.gz" % protobuf_version,
        "https://github.com/protocolbuffers/protobuf/archive/v%s.tar.gz" % protobuf_version,
    ],
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

http_archive(
    name = "rules_proto",
    sha256 = "602e7161d9195e50246177e7c55b2f39950a9cf7366f74ed5f22fd45750cd208",
    strip_prefix = "rules_proto-97d8af4dc474595af3900dd85cb3a29ad28cc313",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_proto/archive/97d8af4dc474595af3900dd85cb3a29ad28cc313.tar.gz",
        "https://github.com/bazelbuild/rules_proto/archive/97d8af4dc474595af3900dd85cb3a29ad28cc313.tar.gz",
    ],
)

load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")

rules_proto_dependencies()

rules_proto_toolchains()