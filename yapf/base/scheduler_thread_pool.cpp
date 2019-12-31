// File Name: scheduler_thread_pool.cpp
// Author: jattlelin
// Created Time: 2019-12-10 20:35:54
// Description:

#include "yapf/base/scheduler_thread_pool.h"

#include <cassert>
#include <functional>
#include <type_traits>

#include "yapf/base/class_register.h"
#include "yapf/base/logging.h"

namespace yapf {

int SchedulerThreadPool::Init(const SchedulerThreadPoolOption &option) {
  assert(!option.scheduler_name.empty());
  size_t start_thread_num = option.thread_num;
  if (start_thread_num <= 4) {
    start_thread_num = 4;
  }
  // job_queue_.init(option.max_queue_size);
  while (start_thread_num--) {
    auto *t = SchedulerThreadClassRegister::GetInstance()->CreateInstance(
        option.scheduler_name, this);
    assert(t != nullptr);
    t->Init(option.thread_option);
    std::unique_ptr<SchedulerThreadBase> holder(t);
    job_threads_.emplace_back(std::move(holder));
  }
  return 0;
}

int SchedulerThreadPool::Submit(JobClosure &&t) {
  // auto *jc = new (std::nothrow) JobClosure;
  // *jc = std::move(t);
  // bool is_empty = job_queue_.size() == 0;
  // if (0 == job_queue_.enqueue(jc, false)) {
  // if (job_queue_.push(jc)) {
  if (0 == job_queue_.push_back(std::move(t))) {
    // if (is_empty) {
    // Notify();
    return 0;
    //}
  }
  return -1;
}

bool SchedulerThreadPool::Get(JobClosure &t, size_t waitms) {
  // Wait(waitms);
  // DAGPF_LOG_INFO << "get job...." << std::endl;
  JobClosure *jc = nullptr;
  // if (0 != job_queue_.dequeue(jc, false)) {
  return job_queue_.pop_front(t, waitms);
  // if (!job_queue_.pop(jc)) {
  //   Wait(waitms);
  //   if (!job_queue_.pop(jc)) {
  //   // if (0 != job_queue_.dequeue(jc, false)) {
  //     DAGPF_LOG_INFO << "get job failed." << std::endl;
  //     return false;
  //   }
  // }
  // DAGPF_LOG_INFO << "get a job." << std::endl;
  // std::unique_ptr<JobClosure> jc_guarder(jc);
  // t.swap(*jc);
  // return true;
}

bool SchedulerThreadPool::Empty() {
  // return job_queue_.size() == 0u;
  return job_queue_.empty();
}

int SchedulerThreadPool::Start() {
  for (auto &t : job_threads_) {
    t->Start();
  }
  return 0;
}

void SchedulerThreadPool::Stop() {
  // for (auto &t : job_threads_) {
  //   t->Stop();
  // }
  job_threads_.clear();
}

}  // namespace yapf

