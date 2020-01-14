package(
    default_visibility = ["//visibility:public"],
)

cc_library(
    name = "class_register",
    hdrs = ["class_register.h"],
    visibility = [ 
        "//visibility:public",
    ],  
)

cc_library(
    name = "dag_processing",
    srcs = ["dag_processing.cpp"],
    hdrs = ["dag_processing.h"],
    deps = [":logging"],
    visibility = [ 
        "//visibility:public",
    ],  
)

cc_library(
    name = "logging",
    hdrs = ["logging.h"],
    deps = [
        "@googleglog//:glog"
        ],
    visibility = [ 
        "//visibility:public",
    ],  
)

cc_library(
    name = "phase_common",
    srcs = ["phase_common.cpp"],
    hdrs = ["phase_common.h"],
    copts = ["-std=c++17"],
    visibility = [ 
        "//visibility:public",
    ],  
)

cc_library(
    name = "phase",
    hdrs = ["phase.h"],
    deps = [
            ":phase_common",
            ":phase_context",
            ],
    visibility = [ 
        "//visibility:public",
    ],  
)

cc_library(
    name = "phase_context",
    hdrs = ["phase_context.h"],
    visibility = [ 
        "//visibility:public",
    ],  
)


cc_library(
    name = "phase_scheduler",
    srcs = ["phase_scheduler.cpp"],
    hdrs = ["phase_scheduler.h"],
    deps = [":class_register",
            ":dag_processing",
            ":phase",
            ":phase_common",
            ":phase_context",
            ":scheduler_thread_pool",
            ":timer_thread",
            "//yapf/flow_control:FlowControlFactory",
            ":logging",
            ],
    copts = ["-fconcepts"],
    visibility = [ 
        "//visibility:public",
    ],
)

cc_library(
    name = "scheduler_thread",
    srcs = ["scheduler_thread.cpp"],
    hdrs = ["scheduler_thread.h"],
    deps = [
            ":scheduler_thread_pool",
            ],
    alwayslink = 1,
    visibility = [ 
        "//visibility:public",
    ],  
)

cc_library(
    name = "scheduler_thread_pool",
    srcs = ["scheduler_thread_pool.cpp"],
    hdrs = ["scheduler_thread_pool.h"],
    copts = ["-fconcepts"],
    deps = [
            "//yapf/flow_control:safe_singleton",
            "//yapf/flow_control:array_lock_free_queue",
            ":logging",
            ":utils",
            ":class_register",
           ],
    visibility = [ 
        "//visibility:public",
    ],  
)

## cc_library(
##     name = "taf_co_thread",
##     srcs = ["taf_co_thread.cpp"],
##     hdrs = ["taf_co_thread.h"],
##     deps = [
##          "@taf//:taf_lib",
##          "@taf//:tafutil_lib",
##         ],
##     visibility = [ 
##         "//visibility:public",
##     ],  
## )

cc_library(
    name = "timeout_queue",
    hdrs = ["timeout_queue.h"],
    deps = [":utils"],
    visibility = [ 
        "//visibility:public",
    ],  
)

cc_library(
    name = "timer_thread",
    hdrs = ["timer_thread.h"],
    deps = [
         ":logging",
         ":scheduler_thread_pool",
         ":timeout_queue",
         ":utils",
        ],
    visibility = [ 
        "//visibility:public",
    ],  
)

cc_library(
    name = "utils",
    hdrs = ["utils.h"],
    deps = [":logging"],
    visibility = [ 
        "//visibility:public",
    ]  
)

cc_test(
    name = "phase_common_test",
    srcs = ["phase_common_test.cc"],
    deps = [
        ":phase_common",
        ":logging",
        "@googletest//:gtest_main"
        ],
)

cc_test(
    name = "phase_test",
    srcs = ["phase_test.cc"],
    deps = [
        ":phase",
        ":phase_scheduler",
        ":logging",
        "@googletest//:gtest_main"
        ],
)

cc_test(
    name = "dag_processing_test",
    srcs = ["dag_processing_test.cc"],
    deps = [
        ":dag_processing",
        ":phase_common",
        ":logging",
        "@googletest//:gtest_main"
        ],
)

cc_test(
    name = "utils_test",
    srcs = ["utils_test.cc"],
    deps = [
        ":utils",
        ":logging",
        "@googletest//:gtest_main"
        ],
)

cc_test(
    name = "timeout_queue_test",
    srcs = ["timeout_queue_test.cc"],
    deps = [
        ":timeout_queue",
        ":logging",
        "@googletest//:gtest_main"
        ],
)

cc_test(
    name = "phase_scheduler_test",
    srcs = ["phase_scheduler_test.cc"],
    deps = [
        ":phase_scheduler",
        ":scheduler_thread",
        ":logging",
        "@googletest//:gtest_main"
        ],
    copts = ["-D__PHASE_NAMESPACE=\"yapf\""]
)
