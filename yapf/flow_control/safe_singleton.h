// File Name: SafeSingleton.h
// Author: jattlelin
// Created Time: 2018-09-03 16:08:47
// Description:

#ifndef _SAFESINGLETON_H
#define _SAFESINGLETON_H

#include <atomic>
#include <memory>
#include <mutex>

template <typename T>
class SafeSingleton {
  SafeSingleton(const SafeSingleton&) = delete;
  SafeSingleton& operator=(const SafeSingleton&) = delete;

 public:
  SafeSingleton() {}
  static T* GetInstance() {
    static std::mutex s_mutex;
    T* tmp = s_instance.load(std::memory_order_relaxed);
    std::atomic_thread_fence(std::memory_order_acquire);
    if (tmp != nullptr) {
      return tmp;
    }
    std::lock_guard<std::mutex> locker(s_mutex);
    tmp = s_instance.load(std::memory_order_relaxed);
    if (tmp == nullptr) {
      tmp = new T;
      std::atomic_thread_fence(std::memory_order_release);
      s_instance.store(tmp, std::memory_order_relaxed);
    }
    return tmp;
  }

 private:
  inline static std::atomic<T*> s_instance{nullptr};
};

#endif
