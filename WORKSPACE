load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "new_git_repository")

## git_repository(
##     name = "googlegtest",
##     remote = "https://github.com/google/googletest",
##     commit = "703bd9caab50b139428cea1aaff9974ebee5742e",
## )

new_git_repository(
    name = "googletest",
    build_file = "gmock.BUILD",
    remote = "https://github.com/google/googletest",
    tag = "release-1.8.0",
)

git_repository(
    name = "googleglog",
    remote = "https://github.com/google/glog",
    commit = "96a2f23dca4cc7180821ca5f32e526314395d26a",
)

git_repository(
    name = "com_github_gflags_gflags",
    remote = "https://github.com/gflags/gflags",
    commit = "e171aa2d15ed9eb17054558e0b3a6a413bb01067",
)

local_repository(
    name = "taf",
    path = "dep/taf/",
)
