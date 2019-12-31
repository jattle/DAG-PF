// File Name: scheduler_thread.cpp
// Author: jattlelin
// Created Time: 2019-12-10 20:11:33
// Description:
//
#include "yapf/base/scheduler_thread.h"

namespace yapf {

int SchedulerThread::Init(const SchedulerThreadOption& option) { return 0; }

int SchedulerThread::Start() {
  static std::mutex s_mutex;
  bool flag = has_started_.load();
  if (flag) return 0;
  std::lock_guard<std::mutex> locker(s_mutex);
  flag = has_started_.load();
  if (flag) return 0;
  thread_ = std::thread{&SchedulerThread::Run, this};
  has_started_.store(true);
  return 0;
}

int SchedulerThread::Stop() {
  has_terminated_.store(true);
  return 0;
}

SchedulerThread::~SchedulerThread() {
  Stop();
  if (thread_.joinable()) {
    thread_.join();
  }
}

void SchedulerThread::Run() {
  while (!has_terminated_.load()) {
    JobClosure jc;
    if (!pool_->Empty()) {
      DAGPF_LOG_INFO << "queue is not empty." << std::endl;
      pool_->Get(jc, 0);
    } else {
      DAGPF_LOG_INFO << "queue is empty." << std::endl;
      pool_->Get(jc, 50);
    }
    if (jc) {
      try {
        jc();
      } catch (...) {
      }
    }
  }
}

struct NormalThreadCreatorRegister {
  NormalThreadCreatorRegister() {
    SchedulerThreadClassRegister::GetInstance()->RegisterCreator(
        "default",
        [](SchedulerThreadPool* pool) { return new SchedulerThread(pool); });
  }
} normal_thread_register_;

}  // namespace yapf
