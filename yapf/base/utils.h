// File Name: utils.h
// Author: jattlelin
// Created Time: 2019-11-15 11:16:47
// Description: 一些通用工具类函数

#ifndef SRC_UTILS_H_
#define SRC_UTILS_H_

#include <chrono>
#include <condition_variable>
#include <deque>
#include <iostream>
#include <mutex>
#include <random>
#include <sstream>
#include <string>

#include "yapf/base/logging.h"

namespace yapf {

struct Utils {
  // get rand int in [start, end]
  template <typename T>
  static int getRandomInt(T start, T end) {
    static_assert(std::is_integral<T>::value, "T must be integral.");
    std::random_device r;
    std::default_random_engine e1(r());
    std::uniform_int_distribution<T> uniform_dist(start, end);
    return uniform_dist(e1);
  }

  // get now milliseconds
  static uint64_t getNowMs() {
    auto p = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               p.time_since_epoch())
        .count();
  }

  static uint64_t getNow() {
    auto p = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::seconds>(
               p.time_since_epoch())
        .count();
  }

  template <typename T>
  static std::string tostr(const T &item) {
    std::ostringstream oss;
    oss << item;
    return oss.str();
  }

  // join
  template <typename InputIterator>
  static std::string join(InputIterator first, InputIterator end,
                          const std::string &sep = ",") {
    std::string result;
    for (InputIterator iter = first; iter != end; ++iter) {
      if (iter != first) result.append(sep);
      result.append(tostr(*iter));
    }
    return result;
  }

  template <typename T>
  class SimpleBlockingQueue {
   public:
    int push_back(T &&t) {
      {
        std::unique_lock<std::mutex> locker(m_mutex);
        m_queue.push_back(std::move(t));
      }
      m_cond.notify_one();
      return 0;
    }

    bool pop_front(T &t, size_t waitms = 0u) {
      if (empty()) {
        std::unique_lock<std::mutex> locker(m_cond_mutex);
        m_cond.wait_for(locker, std::chrono::milliseconds(waitms));
        /// DAGPF_LOG_DEBUG << "wait for " << Utils::getNowMs()-startms << " ms"
        /// << endl;
      }
      std::unique_lock<std::mutex> locker(m_mutex);
      if (m_queue.empty()) {
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

};      // namespace yapf
#endif  // SRC_UTILS_H_

