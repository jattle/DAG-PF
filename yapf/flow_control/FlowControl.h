// File Name: FlowControl.h
// Author: jattlelin
// Created Time: 2018-08-01 16:15:12
// Description:
//
// 实现流控策略，目前可选策略(滑动窗口)
// 被限频时调用方可以选择是否提交后台延迟处理
// 比如调用方使用函数int echoInt(int,int)执行操作，可按如下示例调用：
// //限制100ms内只能有1000次调用
// FlowController ctl(100, 1000);
// if(ctl.rateLimited())
// {
// 	ctl.delay(echoInt, 1, 2);
// }else
// {
// 	echoInt(1, 2);
// }
// 依赖：使用std=c++14编译

#ifndef _FLOWCONTROL_H
#define _FLOWCONTROL_H

#include <chrono>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>

#include "yapf/flow_control/SlidingWindowCounter.h"
#include "yapf/flow_control/array_lock_free_queue.h"
#include "yapf/base/logging.h"
#include "yapf/base/utils.h"

///
/// QUEUE_LEN must be power of 2, eg 1024, 2048, ...32768, 65536
//
template <typename ControlType = SecondSlidingWindowCounter,
          unsigned int QUEUE_LEN = 32768>
class FlowController {
 public:
  // windowSize:n ms
  // maxFlowSize: max reqs allowed in windowSize ms
  explicit FlowController(size_t windowSize, size_t maxFlowSize) {
    /// if(capacity < 1) capacity = queueCapacity;
    m_controller = new ControlType(windowSize, maxFlowSize);
  }

  ~FlowController() {
    m_stopFlag.store(true);
    if (m_worker.joinable()) {
      m_worker.join();
    }
    delete m_controller;
  }

  bool rateLimited() {
    bool flag = m_started.load();
    if (!flag) {
      std::unique_lock<std::mutex> locker(m_lock);
      flag = m_started.load();
      if (!flag) {
        m_worker = std::thread{&FlowController::backgroundRedo, this};
        /// m_worker.swap(tmpThread);
        flag = true;
        m_started.store(flag);
      }
    }
    return m_controller->inc() != 0;
  }

  template <typename _Callable, typename... _Args>
  int delay(long id, size_t timeout, _Callable &&callable, _Args &&... args) {
    m_queue.push(makeWrapper(id, timeout, nullptr,
                             std::bind(std::forward<_Callable>(callable),
                                       std::forward<_Args>(args)...)));
    m_cond.notify_all();
    return 0;
  }

  template <typename _Callable, typename... _Args>
  int delay2(long id, size_t timeout, std::function<void(long, size_t)> cb_fn,
             _Callable &&callable, _Args &&... args) {
    m_queue.push(makeWrapper(id, timeout, cb_fn,
                             std::bind(std::forward<_Callable>(callable),
                                       std::forward<_Args>(args)...)));
    m_cond.notify_all();
    return 0;
  }

 private:
  FlowController(const FlowController &) = delete;
  FlowController &operator=(const FlowController &) = delete;

  struct FuncWrapperBase {
    virtual ~FuncWrapperBase() {}
    virtual void run() = 0;
    virtual void timeout() = 0;
    long m_id;
    size_t m_timeout;
    std::function<void(long, size_t)> m_cb_fn;
    int64_t m_enterTimestamp;
  };
  template <typename _Callable>
  struct FuncWrapper : public FuncWrapperBase {
    explicit FuncWrapper(long id, size_t timeout,
                         std::function<void(long, size_t)> fn, _Callable &&func)
        : _M_Func(std::forward<_Callable>(func)) {
      FuncWrapperBase::m_id = id;
      FuncWrapperBase::m_timeout = timeout;
      FuncWrapperBase::m_cb_fn = fn;
      FuncWrapperBase::m_enterTimestamp = yapf::Utils::getNowMs();
    }
    void run() { _M_Func(); }
    void timeout() {
      if (FuncWrapperBase::m_cb_fn != nullptr) {
        FuncWrapperBase::m_cb_fn(FuncWrapperBase::m_id,
                                 FuncWrapperBase::m_timeout);
      }
    }
    _Callable _M_Func;
  };

  template <typename _Callable>
  std::shared_ptr<FuncWrapper<_Callable> > makeWrapper(
      long id, size_t timeout, std::function<void(long, size_t)> fn,
      _Callable &&f) {
    return std::make_shared<FuncWrapper<_Callable> >(
        id, timeout, fn, std::forward<_Callable>(f));
  }

 private:
  void backgroundRedo() {
    size_t redoTimes = 0;
    bool isLimited = true;
    static constexpr auto kMaxSleepMs = 20;
    static constexpr auto kBaseSleepMs = 1;
    auto sleepMs = kBaseSleepMs;
    while (!m_stopFlag.load()) {
      if (m_queue.size() == 0) {
        std::unique_lock<std::mutex> locker(m_lock);
        m_cond.wait_for(locker, m_waitTime * std::chrono::milliseconds(1000));
      }
      if (isLimited) {
        // random jitter
        sleepMs *= 3;
        if (sleepMs > kMaxSleepMs) sleepMs = kMaxSleepMs;
        sleepMs = yapf::Utils::getRandomInt(kBaseSleepMs, sleepMs);
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepMs));
      }
      std::shared_ptr<FuncWrapperBase> item;
      // array lock free version
      if (m_queue.pop(item)) {
        int64_t elapsed = yapf::Utils::getNowMs() - item->m_enterTimestamp;
        if (elapsed > item->m_timeout) {
          isLimited = false;
          // drop this item
          DAGPF_LOG_ERROR << "timeout, drop item: " << item->m_id
                    << ", timeout val = " << item->m_timeout
                    << ", elaspsed: " << elapsed << std::endl;
          try {
            item->timeout();
          } catch (...) {
          }
          continue;
        }
        isLimited = rateLimited();
        if (!isLimited) {
          ++redoTimes;
          DAGPF_LOG_DEBUG << "redo item: " << item->m_id
                    << ", timeout val = " << item->m_timeout
                    << ", elaspsed: " << elapsed << std::endl;
          try {
            item->run();
          } catch (...) {
          }
        } else {
          DAGPF_LOG_DEBUG << "limited. give back item: " << item->m_id << std::endl;
          m_queue.push(item);
        }
      }
    }
    DAGPF_LOG_DEBUG << "redoTimes: " << redoTimes << std::endl;
  }

 private:
  ArrayLockFreeQueue<std::shared_ptr<FuncWrapperBase>, QUEUE_LEN> m_queue;
  ControlType *m_controller{nullptr};
  std::thread m_worker;
  mutable std::mutex m_lock;
  int m_waitTime{3};
  std::condition_variable m_cond;
  std::atomic<bool> m_stopFlag{false};
  std::atomic<bool> m_started{false};
};
#endif

