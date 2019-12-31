// File Name: utils.h
// Author: jattlelin
// Created Time: 2018-07-30 15:01:06
// Description:

#ifndef _UTILS_H
#define _UTILS_H

#include <chrono>
#include <condition_variable>
#include <deque>
#include <iostream>
#include <mutex>
#include <random>
#include <sstream>

#include "yapf/base/logging.h"

class Utils {
 public:
  // get rand int in [start, end]
  static int getRandomInt(int start, int end) {
    static std::random_device r;
    static std::default_random_engine e1(r());
    static std::uniform_int_distribution<int> uniform_dist(start, end);
    return uniform_dist(e1);
  }

  // get now milliseconds
  static int64_t getNowMs() {
    auto p = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               p.time_since_epoch())
        .count();
  }

  static int64_t getNow() {
    auto p = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::seconds>(
               p.time_since_epoch())
        .count();
  }

  template <typename T>
  class SimpleBlockingQueue {
   public:
    int push_back(T &&t) {
      {
        std::unique_lock<std::mutex> locker(m_mutex);
        m_queue.push_back(std::move(t));
      }
      m_cond.notify_all();
      return 0;
    }

    bool pop_front(T &t, size_t waitms = 0u) {
      if (empty()) {
        DAGPF_LOG_INFO <<  "queue is empty." << std::endl;
        int64_t startms = Utils::getNowMs();
        std::unique_lock<std::mutex> locker(m_cond_mutex);
        m_cond.wait_for(locker, std::chrono::milliseconds(waitms));
        DAGPF_LOG_INFO << "wait for " << Utils::getNowMs()-startms << " ms"
        << std::endl;
      }
      std::unique_lock<std::mutex> locker(m_mutex);
      if (m_queue.empty()) {
        DAGPF_LOG_DEBUG <<  "empty queue." << std::endl;
        return false;
      }
      t = std::move(m_queue.front());
      m_queue.pop_front();
      return true;
    }

    bool empty() {
      std::unique_lock<std::mutex> locker(m_mutex);
      return m_queue.empty();
    }

   private:
    std::condition_variable m_cond;
    std::mutex m_cond_mutex;
    std::mutex m_mutex;
    std::deque<T> m_queue;
  };
};
#endif

