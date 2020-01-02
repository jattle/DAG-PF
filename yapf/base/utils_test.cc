// File Name: utils_test.cc
// Author: jattlelin
// Created Time: 2019-12-31 17:40:07
// Description:
//
#include "yapf/base/utils.h"

#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "yapf/base/logging.h"

namespace yapf {

TEST(UtilsTest, getRandomInt) {
  auto ret = Utils::getRandomInt(0, 0);
  EXPECT_EQ(ret, 0);
  ret = Utils::getRandomInt(1, 100);
  EXPECT_TRUE(ret <= 100 and ret >= 1);
}

TEST(UtilsTest, tostr) {
  auto ret = Utils::tostr(1);
  EXPECT_STREQ(ret.c_str(), "1");
  ret = Utils::tostr("");
  EXPECT_STREQ(ret.c_str(), "");
}

TEST(UtilsTest, join) {
  std::vector v{1, 2, 3, 4};
  auto ret = Utils::join(v.begin(), v.end());
  EXPECT_STREQ(ret.c_str(), "1,2,3,4");
}

TEST(UtilsTest, SimpleBlockingQueue) {
  Utils::SimpleBlockingQueue<int> q;
  EXPECT_TRUE(q.empty());
  EXPECT_EQ(q.push_back(1), 0);
  int t;
  EXPECT_FALSE(q.empty());
  EXPECT_TRUE(q.pop_front(t));
  EXPECT_EQ(t, 1);
}

TEST(UtilsTest, SimpleBlockingQueueThreading) {
  Utils::SimpleBlockingQueue<int> q;
  std::thread t1{[&q]() {
    for (int i = 0; i < 10; ++i) {
      q.push_back(std::move(i));
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  }};
  std::thread t2{[&q]() {
    for (int i = 0; i < 10; ++i) {
      int t;
      q.pop_front(t, 100);
    }
  }};
  t1.join();
  t2.join();
  EXPECT_TRUE(q.empty());
}

}  // namespace yapf
