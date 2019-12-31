// File Name: logging.h
// Author: jattlelin
// Created Time: 2019-10-27 17:26:06
// Description: 默认使用glog实现框架日志打印
// 使用者可以替换成其他库

#ifndef SRC_LOGGING_H_
#define SRC_LOGGING_H_

#include <glog/logging.h>

#define DAGPF_LOG_INFO LOG(INFO)
#define DAGPF_LOG_DEBUG LOG(INFO)
#define DAGPF_LOG_ERROR LOG(ERROR)
#define DAGPF_LOG_WARN LOG(WARN)


#endif  //

