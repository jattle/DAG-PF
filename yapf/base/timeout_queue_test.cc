// File Name: timeout_queue_test.cc
// Author: jattlelin
// Created Time: 2019-12-31 19:38:55
// Description: 

#include "yapf/base/timeout_queue.h"

#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "yapf/base/logging.h"

namespace yapf {
  TEST(TimeoutQueueTest, GeneralOp) {
    TimeoutQueue<int> q;
    // generateId
    EXPECT_EQ(q.generateId(), 1u);
    auto id = q.generateId();
    // empty
    EXPECT_TRUE(q.empty());
    // erase
    EXPECT_EQ(-1, q.erase(1u));
    // push
    q.push(id, 2, 1000);
    q.push(q.generateId(), 3, 1000);
    q.push(q.generateId(), 4, 1000);
    EXPECT_FALSE(q.empty());
    EXPECT_EQ(q.size(), 3u);
    // timeout
    std::vector<int> vec;
    EXPECT_EQ(-1, q.timeout(&vec));
    // erase
    EXPECT_EQ(0, q.erase(2u));
    EXPECT_EQ(q.size(), 2u);
    // pop
    int item;
    EXPECT_EQ(0, q.pop(3, item));
    EXPECT_EQ(item, 3);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    EXPECT_EQ(0, q.timeout(&vec));
    EXPECT_EQ(1u, vec.size());
  }
}
