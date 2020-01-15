// File Name: phase_scheduler_test.cc
// CopyRight 2020
// Author: jattlelin
// Created Time: 2020-01-13 17:28:57
// Description:

#include "yapf/base/phase_scheduler.h"

#include <chrono>
#include <future>
#include <iostream>
#include <mutex>
#include <thread>

#include "gtest/gtest.h"

namespace yapf {

struct TestContext : public yapf::PhaseContext {
  std::string GetLogHead() const override {
    std::string log_buffer;
    log_buffer.append("TestContext")
        .append(": ret = ")
        .append(std::to_string(ret));
    return log_buffer;
  }
  // may have contention
  std::vector<std::string> executed_phases;
  std::mutex local_mutex;
  std::string redo_phase;
  int ret{-1};
  std::promise<int> promise_val;
};

class StartPhase : public yapf::Phase {
 public:
  StartPhase() {}

 protected:
  int DoProcess(yapf::PhaseContextPtr context_ptr,
                const yapf::PhaseParamDetail &detail) override {
    auto biz_ctx = ToBizCtxPtr<TestContext>(context_ptr);
    {
      std::unique_lock<std::mutex> locker(biz_ctx->local_mutex);
      biz_ctx->executed_phases.emplace_back(this->GetName());
    }
    return NotifyDone(0);
  }
};

REGISTER_CLASS(yapf, Phase, yapf, StartPhase);

class EndPhase : public yapf::Phase {
 public:
  EndPhase() {}

 protected:
  int DoProcess(yapf::PhaseContextPtr context_ptr,
                const yapf::PhaseParamDetail &detail) override {
    auto biz_ctx = ToBizCtxPtr<TestContext>(context_ptr);
    {
      std::unique_lock<std::mutex> locker(biz_ctx->local_mutex);
      biz_ctx->executed_phases.emplace_back(this->GetName());
    }
    biz_ctx->ret = 0;
    biz_ctx->promise_val.set_value(0);
    return NotifyDone(0);
  }
};

REGISTER_CLASS(yapf, Phase, yapf, EndPhase);

class APhase : public yapf::Phase {
 public:
  APhase() {}

 protected:
  int DoProcess(yapf::PhaseContextPtr context_ptr,
                const yapf::PhaseParamDetail &detail) override {
    auto biz_ctx = ToBizCtxPtr<TestContext>(context_ptr);
    {
      std::unique_lock<std::mutex> locker(biz_ctx->local_mutex);
      biz_ctx->executed_phases.emplace_back(this->GetName());
    }
    return NotifyDone(0);
  }
};

REGISTER_CLASS(yapf, Phase, yapf, APhase);

class BPhase : public yapf::Phase {
 public:
  BPhase() {}

 protected:
  int DoProcess(yapf::PhaseContextPtr context_ptr,
                const yapf::PhaseParamDetail &detail) override {
    auto biz_ctx = ToBizCtxPtr<TestContext>(context_ptr);
    {
      std::unique_lock<std::mutex> locker(biz_ctx->local_mutex);
      biz_ctx->executed_phases.emplace_back(this->GetName());
    }
    return NotifyDone(0);
  }
};

REGISTER_CLASS(yapf, Phase, yapf, BPhase);

class CPhase : public yapf::Phase {
 public:
  CPhase() {}

 protected:
  int DoProcess(yapf::PhaseContextPtr context_ptr,
                const yapf::PhaseParamDetail &detail) override {
    auto biz_ctx = ToBizCtxPtr<TestContext>(context_ptr);
    {
      std::unique_lock<std::mutex> locker(biz_ctx->local_mutex);
      biz_ctx->executed_phases.emplace_back(this->GetName());
    }
    return NotifyDone(0);
  }
};

REGISTER_CLASS(yapf, Phase, yapf, CPhase);

class DPhase : public yapf::Phase {
 public:
  DPhase() {}

 protected:
  int DoProcess(yapf::PhaseContextPtr context_ptr,
                const yapf::PhaseParamDetail &detail) override {
    auto biz_ctx = ToBizCtxPtr<TestContext>(context_ptr);
    {
      std::unique_lock<std::mutex> locker(biz_ctx->local_mutex);
      biz_ctx->executed_phases.emplace_back(this->GetName());
    }
    return NotifySkip();
  }
};

REGISTER_CLASS(yapf, Phase, yapf, DPhase);

class EPhase : public yapf::Phase {
 public:
  EPhase() {}

 protected:
  int DoProcess(yapf::PhaseContextPtr context_ptr,
                const yapf::PhaseParamDetail &detail) override {
    auto biz_ctx = ToBizCtxPtr<TestContext>(context_ptr);
    if (this->GetRedoRetryTimes() > 0) {
      biz_ctx->redo_phase = this->GetName();
    }
    if (!redo_flag_) {
      {
        std::unique_lock<std::mutex> locker(biz_ctx->local_mutex);
        biz_ctx->executed_phases.emplace_back(this->GetName());
      }
      redo_flag_ = true;
      return NotifyRedo();
    }
    return NotifyDone(0);
  }

 private:
  bool redo_flag_{false};
};

REGISTER_CLASS(yapf, Phase, yapf, EPhase);

class PhaseSchedulerTest : public ::testing::Test {
 public:
  void SetUp() override {
    // global init
    SchedulerOption scheduler_option;
    scheduler_option.enable_statis = true;
    scheduler_option.enable_thread_pool = true;
    scheduler_option.pool_option.scheduler_name = "default";
    scheduler_option.pool_option.thread_num = 2;
    scheduler_option.pool_option.max_queue_size = 100;
    PhaseScheduler::GlobalInit(scheduler_option);
    // create a reused scheduler
    reused_scheduler.SetPhaseNameSpace("yapf");
    EXPECT_EQ(0, InitScheduler(exprs, alias_map, reused_scheduler));
  }
  void TearDown() override {}

 protected:
  PhaseScheduler reused_scheduler;
  inline static const std::vector<std::string> exprs{"a->b", "b->c", "b->d",
                                                     "e"};
  inline static const std::unordered_map<std::string, std::string> alias_map{
      {"a", "APhase"},
      {"b", "BPhase"},
      {"c", "CPhase"},
      {"d", "DPhase"},
      {"e", "EPhase(redo:true,redo_retry_interval:200,redo_retry_times:1)"}};
};

TEST_F(PhaseSchedulerTest, StartScheduler) {
  // start one scheduler, which is copied from reused_scheduler
  auto test_context = new TestContext();
  PhaseContextPtr ctx_ptr{test_context};
  std::future<int> f = test_context->promise_val.get_future();
  EXPECT_EQ(0, StartScheduler(reused_scheduler, ctx_ptr));
  DAGPF_LOG_INFO << "f val: " << f.get() << std::endl;
  EXPECT_EQ(0, test_context->ret);
  // start, end, a, b, c, d, e
  EXPECT_EQ(7u, test_context->executed_phases.size());
  EXPECT_EQ(std::string("e"), test_context->redo_phase);
}

}  // namespace yapf

