package(
    default_visibility = ["//visibility:public"],
)

cc_library(
    name = "FlowControl",
    hdrs = ["FlowControl.h"],
    deps = [
            "//yapf/base:logging",
            "//yapf/base:utils",
            ":SlidingWindowCounter",
            ":array_lock_free_queue",
            ],
    visibility = [ 
        "//visibility:public",
    ],  
)

cc_library(
    name = "FlowControlFactory",
    srcs = ["FlowControlFactory.cpp"],
    hdrs = ["FlowControlFactory.h"],
    deps = [
            "//yapf/base:logging",
            ":SlidingWindowCounter",
            ":FlowControl",
            ],
    visibility = [ 
        "//visibility:public",
    ],  
)

cc_library(
    name = "SlidingWindowCounter",
    srcs = ["SlidingWindowCounter.cpp"],
    hdrs = ["SlidingWindowCounter.h"],
    deps = [
            "//yapf/base:utils"
           ],
    visibility = [ 
        "//visibility:public",
    ],  
)

cc_library(
    name = "array_lock_free_queue",
    hdrs = [
            "array_lock_free_queue.h",
            "array_lock_free_queue_impl.h",
            "atomic_ops.h",
            ],
    visibility = [ 
        "//visibility:public",
    ],  
)

cc_library(
    name = "safe_singleton",
    hdrs = ["safe_singleton.h"],
    visibility = [ 
        "//visibility:public",
    ],  
)
