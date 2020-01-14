// File Name: scheduler_thread_pool.h
// Author: jattlelin
// Created Time: 2019-11-15 11:43:29
// Description: 调度线程池及相关选项定义

#ifndef SCHEDULER_THREAD_POOL_H_
#define SCHEDULER_THREAD_POOL_H_

#include <semaphore.h>

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "yapf/base/logging.h"
// #include "yapf/base/tc_lockfree_queue.h"
#include "yapf/base/utils.h"
#include "yapf/flow_control/array_lock_free_queue.h"
#include "yapf/flow_control/safe_singleton.h"

namespace yapf {

struct SchedulerThreadOption {
  size_t coro_run_num{128};
  size_t coro_max_num{1024};
  size_t stack_size{131072};
};

struct SchedulerThreadPoolOption {
  std::string scheduler_name{"default"};
  uint32_t thread_num{4};
  uint32_t max_queue_size{10000};
  SchedulerThreadOption thread_option;
};

// base interface
// implementation maybe base thread, taf coroutine, trpc coroutine etc
class SchedulerThreadPool;
class SchedulerThreadBase {
 public:
  explicit SchedulerThreadBase(SchedulerThreadPool *pool) : pool_(pool) {}
  virtual ~SchedulerThreadBase() = default;
  virtual int Init(const SchedulerThreadOption &option) = 0;
  virtual int Start() = 0;
  virtual int Stop() = 0;

 protected:
  SchedulerThreadPool *pool_{nullptr};
  std::atomic<bool> has_inited_{false};
  std::atomic<bool> has_terminated_{false};
  std::atomic<bool> has_started_{false};
};

// TODO

using JobClosure = std::function<void(void)>;

class SchedulerThreadPool {
 public:
  SchedulerThreadPool() = default;
  ~SchedulerThreadPool() = default; 
  int Init(const SchedulerThreadPoolOption &option);
  int Submit(JobClosure &&t);
  bool Get(JobClosure &t, size_t);
  bool Empty();
  int Start();
  void Stop();

 private:
  void Notify() {
    // using condition variable
    cond_.notify_one();
  }

  void Wait(uint64_t timeout_ms) {
    if (timeout_ms == 0) return;
    std::unique_lock<std::mutex> locker(cond_mutex_);
    cond_.wait_for(locker, std::chrono::milliseconds(timeout_ms));
  }

 private:
  std::atomic<bool> has_inited_{false};
  std::condition_variable cond_;
  std::mutex cond_mutex_;
  std::vector<std::unique_ptr<SchedulerThreadBase>> job_threads_;
  Utils::SimpleBlockingQueue<JobClosure> job_queue_;
};

class SchedulerThreadClassRegister
    : public SafeSingleton<SchedulerThreadClassRegister> {
 public:
  SchedulerThreadClassRegister() = default;
  void RegisterCreator(const std::string &name, auto &&t) {
    creator_map_.emplace(name, std::move(t));
  }
  SchedulerThreadBase *CreateInstance(const std::string &name,
                                      SchedulerThreadPool *pool) {
    auto iter = creator_map_.find(name);
    if (iter == creator_map_.end()) {
      return nullptr;
    }
    return (iter->second)(pool);
  }

 private:
  std::unordered_map<
      std::string, std::function<SchedulerThreadBase *(SchedulerThreadPool *)>>
      creator_map_;
};

}  // namespace yapf

#endif  // SRC_SCHEDULER_THREAD_POOL_H_

