// File Name: scheduler_thread.h
// Author: jattlelin
// Created Time: 2019-12-10 20:07:44
// Description: 默认调度线程定义

#ifndef SCHEDULER_THREAD_H_
#define SCHEDULER_THREAD_H_

#include <thread>

#include "yapf/base/scheduler_thread_pool.h"

namespace yapf {
class SchedulerThreadPool;
class SchedulerThread : public SchedulerThreadBase {
 public:
  explicit SchedulerThread(SchedulerThreadPool *pool)
      : SchedulerThreadBase(pool) {}
  ~SchedulerThread();
  int Init(const SchedulerThreadOption &option) override;
  int Start() override;
  int Stop() override;

 protected:
  void Run();

 private:
  std::thread thread_;
};
}  // namespace yapf

#endif  // SRC_SCHEDULER_THREAD_H_

