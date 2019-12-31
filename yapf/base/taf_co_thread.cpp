// File Name: taf_co_thread.cpp
// Author: jattlelin
// Created Time: 2019-12-10 20:22:56
// Description:

#include "yapf/base/taf_co_thread.h"

#include <syscall.h>

#ifndef ENABLE_TAF_COROUTINE
#pragma message \
    "TAF coroutine not enabled, Please Set Macro ENABLE_TAF_COROUTINE"
#else

namespace yapf {

TafCoroSchedulerThread::~TafCoroSchedulerThread() {
  Stop();
  if (this->isAlive()) {
    this->getThreadControl().join();
  }
}

int TafCoroSchedulerThread::Init(const SchedulerThreadOption& option) {
  setCoroInfo(option.coro_run_num, option.coro_max_num, option.stack_size);
  return 0;
}

int TafCoroSchedulerThread::Start() { this->start(); }

int TafCoroSchedulerThread::Stop() { has_terminated_.store(true); }
// taf coroutine related
void TafCoroSchedulerThread::handle() {
  int tid = syscall(SYS_gettid);
  int coro_id = this->getCoroSched()->getCoroutineId();
  DAGPF_LOG_DEBUG << "scheduler thread. tid: " << tid
                  << ", coro id: " << coro_id << endl;
  while (!has_terminated_.load()) {
    bool is_need_yield =
        getCoroSched()->getResponseCoroSize() > 0 or running_coro_count_ > 0;
    JobClosure jc;
    if (not pool_->Empty()) {
      pool_->Get(jc, 0);
    } else {
      if (is_need_yield) {
        yield();
      } else {
        pool_->Get(jc, 100);
      }
    }
    if (jc) {
      ++running_coro_count_;
      DAGPF_LOG_DEBUG << "coro get a job. tid: " << syscall(SYS_gettid) 
                      << ", coro id: " << this->getCoroSched()->getCoroutineId() << endl;
      try {
        (jc)();
      } catch (std::exception& ex) {
        DAGPF_LOG_ERROR << "invoke job catch exception: " << ex.what()
                        << std::endl;
      } catch (...) {
        DAGPF_LOG_ERROR << "invoke job catch unknown exception." << std::endl;
      }
      --running_coro_count_;
    } else {
      DAGPF_LOG_DEBUG << "coro get job empty. tid: " << syscall(SYS_gettid) 
                      << ", coro id: " << this->getCoroSched()->getCoroutineId() << endl;
    }
  }
}

struct TafCoThreadCreatorRegister {
  TafCoThreadCreatorRegister() {
    SchedulerThreadClassRegister::GetInstance()->RegisterCreator(
        "taf_coroutine", [](SchedulerThreadPool* pool) {
          return new TafCoroSchedulerThread(pool);
        });
  }
} taf_co_thread_register_;

}  // namespace yapf
#endif

