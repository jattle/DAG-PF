// File Name: taf_co_thread.h
// Author: jattlelin
// Created Time: 2019-12-03 16:26:54
// Description:

#ifndef TAF_CO_THREAD_H_
#define TAF_CO_THREAD_H_

#ifndef ENABLE_TAF_COROUTINE
#pragma message \
    "TAF coroutine not enabled, Please Set Macro ENABLE_TAF_COROUTINE"
#else

#include <thread>

#include "yapf/base/scheduler_thread_pool.h"
#include "servant/CoroutineScheduler.h"

namespace yapf {

class SchedulerThreadPool;

class TafCoroSchedulerThread : public taf::Coroutine,
                               public SchedulerThreadBase {
 public:
  explicit TafCoroSchedulerThread(SchedulerThreadPool *pool)
      : SchedulerThreadBase(pool) {}
  ~TafCoroSchedulerThread();
  int Init(const SchedulerThreadOption &option) override;
  int Start() override;
  int Stop() override;
  // taf coroutine related
  void handle() override;

 private:
  int running_coro_count_{0};
};

}  // namespace yapf
#endif

#endif

