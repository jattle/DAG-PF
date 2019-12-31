// File Name: timeout_queue.h
// Author: jattlelin
// Created Time: 2019-11-14 16:39:47
// Description: 超时队列实现
// 非多线程安全

#ifndef SRC_TIMEOUT_QUEUE_H_
#define SRC_TIMEOUT_QUEUE_H_

#include <sys/time.h>

#include <atomic>
#include <cassert>
#include <chrono>
#include <iostream>
#include <map>
#include <unordered_map>
#include <utility>
#include <vector>

#include "yapf/base/utils.h"

namespace yapf {

template <typename T>
class TimeoutQueue {
  struct TimerInfo;

  struct DataInfo {
    T data;
    typename std::multimap<int64_t, TimerInfo>::iterator timeIter;
  };

  typedef std::multimap<int64_t, TimerInfo> TIME_MAP_TYPE;
  typedef std::unordered_map<size_t, DataInfo> DATA_MAP_TYPE;

  struct TimerInfo {
    typename DATA_MAP_TYPE::iterator dataIter;
  };

 public:
  explicit TimeoutQueue(int timeout = 2000) : m_timeout(timeout) {}

  size_t generateId();

  int push(size_t id, T &&data, int timeout = 2000);

  // 删除特定id
  int erase(size_t id);

  int pop(size_t id, T &item);

  // 删除当前超时数据
  int timeout(std::vector<T> *vec = NULL);

  size_t size() const;

  bool empty() const;

 private:
  TIME_MAP_TYPE m_timeMap;  // 超时表
  DATA_MAP_TYPE m_idMap;    // 数据表
  int64_t m_timeout;        // 默认超时时间
};

template <typename T>
size_t TimeoutQueue<T>::generateId() {
  static std::atomic<size_t> counter;
  return ++counter;
}

template <typename T>
int TimeoutQueue<T>::push(size_t id, T &&data, int timeout) {
  DataInfo dataInfo;
  dataInfo.data = std::move(data);
  TimerInfo timerInfo;
  auto it = m_timeMap.emplace(Utils::getNowMs() + timeout, timerInfo);
  dataInfo.timeIter = it;
  auto dataRet = m_idMap.emplace(id, dataInfo);
  it->second.dataIter = dataRet.first;
  return 0;
}

// 删除特定id
template <typename T>
int TimeoutQueue<T>::erase(size_t id) {
  auto mIter = m_idMap.find(id);
  if (mIter == m_idMap.end()) {
    return -1;
  }
  m_timeMap.erase(mIter->second.timeIter);
  m_idMap.erase(mIter);
  return 0;
}

// 删除当前超时数据
template <typename T>
int TimeoutQueue<T>::timeout(std::vector<T> *vec) {
  if (m_timeMap.empty()) {
    return 1;
  }
  int64_t nowms = Utils::getNowMs();
  auto timeIter = m_timeMap.begin();
  if (timeIter->first > nowms) {
    // 没有超时数据
    return -1;
  }
  for (; timeIter != m_timeMap.end() && timeIter->first <= nowms;) {
    if (vec != nullptr) {
      vec->emplace_back(std::move(timeIter->second.dataIter->second.data));
    }
    m_idMap.erase(timeIter->second.dataIter);
    m_timeMap.erase(timeIter++);
  }
  return 0;
}

template <typename T>
size_t TimeoutQueue<T>::size() const {
  return m_timeMap.size();
}

template <typename T>
bool TimeoutQueue<T>::empty() const {
  return m_timeMap.empty();
}

// 弹出特定id
template <typename T>
int TimeoutQueue<T>::pop(size_t id, T &item) {
  auto mIter = m_idMap.find(id);
  if (mIter == m_idMap.end()) {
    return -1;
  }
  item = mIter->second.data;
  m_timeMap.erase(mIter->second.timeIter);
  m_idMap.erase(mIter);
  return 0;
}

};      // namespace yapf
#endif  // SRC_TIMEOUT_QUEUE_H_

