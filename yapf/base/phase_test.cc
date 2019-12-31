// File Name: phase_test.cc
// Author: jattlelin
// Created Time: 2019-12-26 14:58:03
// Description:

#include "yapf/base/phase.h"

#include <chrono>
#include <iostream>
#include <thread>

#include "gtest/gtest.h"

namespace yapf {
struct TestPhaseContext : public PhaseContext {
  void *rpc_context_ptr{};
};

class TestPhase : public Phase {
 public:
  int DoProcess(PhaseContextPtr context_ptr,
                const PhaseParamDetail &detail) override {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    return NotifyDone(0);
  }
};

TEST(PhaseTest, Phase) {
  PhasePtr p{new TestPhase()};
  p->SetName("TestPhase");
  auto t = new TestPhaseContext();
  PhaseContextPtr ctx_ptr{t};
  PhaseParamDetail detail;
  auto f = p->Run(ctx_ptr, detail);
  EXPECT_EQ(0, f.GetValue());
}
}  // namespace yapf

