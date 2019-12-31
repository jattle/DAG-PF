// File Name: timer_thread.h
// Author: jattlelin
// Created Time: 2019-11-14 16:51:01
// Description: 超时线程定义

#ifndef SRC_TIMER_THREAD_H_
#define SRC_TIMER_THREAD_H_

#include <semaphore.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>
#include <tuple>
#include <vector>

#include "yapf/base/logging.h"
#include "yapf/base/scheduler_thread_pool.h"
#include "yapf/base/timeout_queue.h"
#include "yapf/base/utils.h"

namespace yapf {

using TimerCBType = std::function<void(void)>;

class TimerThread {
  TimerThread(const TimerThread&) = delete;
  TimerThread& operator=(const TimerThread&) = delete;

 public:
  TimerThread() { sem_init(&sem_, 0, 0); }
  ~TimerThread() {
    stop();
    if (m_thread.joinable()) m_thread.join();
    sem_destroy(&sem_);
  }
  void start() {
    bool tmp = m_startFlag.load();
    if (tmp) return;
    static std::mutex s_mutex;
    std::lock_guard<std::mutex> locker(s_mutex);
    tmp = m_startFlag.load();
    if (tmp) return;
    m_thread = std::thread{&TimerThread::run, this};
    m_startFlag.store(true);
  }

  void stop() { m_stopFlag.store(true); }

  int push(size_t id, auto&& cb, int timeout = 2000) {
    static_assert(
        std::is_convertible<decltype(cb), std::function<void(void)>>::value,
        "cb type must match void(void)");
    int ret = -1;
    {
      std::lock_guard<std::mutex> locker(m_mutex);
      ret = m_timedQueue.push(id, cb, timeout);
    }
    // m_cond.notify_all();
    notify();
    return ret;
  }

  int erase(size_t id) {
    std::lock_guard<std::mutex> locker(m_mutex);
    return m_timedQueue.erase(id);
  }

 private:
  void notify() {
    int val = 0;
    sem_getvalue(&sem_, &val);
    if (val < 1) {
      sem_post(&sem_);
    }
  }

  void wait(uint64_t timeout_ms) {
    struct timespec ts;
    uint64_t abstime_ms = yapf::Utils::getNowMs() + timeout_ms;
    ts.tv_sec = abstime_ms / 1000;
    ts.tv_nsec = (abstime_ms % 1000) * 1000 * 1000;
    sem_timedwait(&sem_, &ts);
  }

  int run() {
    while (!m_stopFlag.load()) {
      // scan
      wait(100);
      if (m_stopFlag.load()) break;
      std::vector<TimerCBType> vec;
      {
        std::lock_guard<std::mutex> locker(m_mutex);
        m_timedQueue.timeout(&vec);
      }
      if (!vec.empty()) {
        // exec timeout action
        for (auto& cb : vec) {
          try {
            cb();
          } catch (...) {
          }
        }
      }
    }
    return 0;
  }

 private:
  std::thread m_thread;
  std::atomic<bool> m_startFlag{false};
  std::mutex m_mutex;  // data access mutex
  // std::condition_variable m_cond;
  // std::mutex m_condMutex;
  sem_t sem_;
  std::atomic<bool> m_stopFlag{false};
  TimeoutQueue<TimerCBType> m_timedQueue;
};

};  // namespace yapf

#endif  // SRC_TIMER_THREAD_H_

