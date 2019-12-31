// File Name: SlidingWindowCounter.cpp
// Author: jattlelin
// Created Time: 2018-07-28 18:40:09
// Description:

#include "yapf/flow_control/SlidingWindowCounter.h"

#include <iostream>

#include "yapf/base/utils.h"

int SecondSlidingWindowCounter::inc() {
  int64_t nowms = yapf::Utils::getNowMs();
  auto timeIndex = nowms % kWindowArraySize;
  auto oldStart = 0;
  auto lf = [&] {
    //初始化
    //新创建时间窗口
    slidingWindow.counter.fetch_and(0ul);
    slidingWindow.windowArr[timeIndex].counter.fetch_and(0ul);
    slidingWindow.windowStart = timeIndex;
    slidingWindow.windowEnd = (timeIndex + m_windowSize) % kWindowArraySize;
    slidingWindow.windowCursor = timeIndex;
    slidingWindow.windowArr[slidingWindow.windowStart].accessTime = nowms;
    slidingWindow.windowArr[slidingWindow.windowEnd].accessTime =
        nowms + m_windowSize;
    slidingWindow.windowArr[timeIndex].counter.fetch_add(1ul);
    slidingWindow.windowArr[timeIndex].accessTime = nowms;
    slidingWindow.counter.fetch_add(1ul);
  };
  do {
    std::unique_lock<std::mutex> lock(m_lock);
    oldStart = slidingWindow.windowStart;
    //当前无窗口存在
    if (slidingWindow.windowEnd == -1) {
      lf();
      return kRetOK;
    }
    // windowStart:最左时间点，cursor:最右时间点
    slidingWindow.windowArr[timeIndex].accessTime = nowms;
    auto interval =
        nowms - slidingWindow.windowArr[slidingWindow.windowEnd].accessTime;
    if (interval > 0) {
      if (nowms >
          slidingWindow.windowArr[slidingWindow.windowCursor].accessTime +
              m_windowSize) {
        //当前时间点超出了时间窗口
        //重新创建窗口
        lf();
        return kRetOK;
      }
      if (timeIndex > slidingWindow.windowStart) {
        if (slidingWindow.windowStart + m_windowSize >= timeIndex) break;
      } else {
        if (slidingWindow.windowStart + m_windowSize >=
            timeIndex + kWindowArraySize)
          break;
      }
      //当前时间点在[windowStart, end]中, 滑动窗口
      //移动windowStart到第一个有效的时间节点
      slidingWindow.windowStart += interval;
      auto startIndex = slidingWindow.windowStart;
      for (; startIndex != timeIndex;) {
        if (slidingWindow.windowArr[startIndex].counter.load() > 0) break;
        startIndex = (startIndex + 1) % kWindowArraySize;
      }
      slidingWindow.windowStart = startIndex;
      slidingWindow.windowEnd += interval;
      for (auto i = oldStart; i < slidingWindow.windowStart; ++i) {
        auto cnt =
            slidingWindow.windowArr[i % kWindowArraySize].counter.fetch_and(
                0ul);
        if (cnt > 0) slidingWindow.counter.fetch_add(-cnt);
      }
      slidingWindow.windowStart = slidingWindow.windowStart % kWindowArraySize;
      slidingWindow.windowEnd = slidingWindow.windowEnd % kWindowArraySize;
      slidingWindow.windowArr[slidingWindow.windowEnd].accessTime = nowms;
      //更新cursor
      slidingWindow.windowCursor = timeIndex;
      break;
    } else {
      //落在窗口中
      //更新cursor
      slidingWindow.windowCursor = timeIndex;
      break;
    }
  } while (0);
  if (slidingWindow.counter + 1 > m_maxFlowSize) return kRetFlowLimit;
  slidingWindow.counter.fetch_add(1ul);
  slidingWindow.windowArr[timeIndex].counter.fetch_add(1ul);
  return kRetOK;
}

