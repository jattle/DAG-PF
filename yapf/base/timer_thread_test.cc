// File Name: timer_thread_test.cc
// Author: jattlelin
// Created Time: 2019-12-31 19:23:03
// Description:

#include "yapf/base/timer_thread.h"

#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "yapf/base/logging.h"

namespace yapf {

TEST(TimerThread, GeneralOp) {
  TimerThread t;
  t.start();
  EXPECT_EQ(t.size(), 0u);
  EXPECT_EQ(-1, t.erase());
  std::atomic<size_t> tid{0};
  std::atomic<bool> running_flag{false};
  t.push(
      ++tid,
      [&running_flag]() {
        running_flag.store(true, std::memory_order_relaxed);
      },
      100);
  EXPECT_EQ(t.size(), 1u);
  std::this_thread::sleep_for(std::chrono::milliseconds(150));
  EXPECT_TRUE(running_flag.load(std::memory_order_relaxed));
  EXPECT_EQ(t.size(), 0u);
  auto ttid = ++tid;
  t.push(
      ttid, []() { running_flag.store(false, std::memory_order_relaxed); },
      200);
  EXPECT_EQ(0, t.erase(ttid));
  EXPECT_FALSE(running_flag.load(std::memory_order_relaxed));
  EXPECT_EQ(-1, t.erase(ttid));
}

}  // namespace yapf
