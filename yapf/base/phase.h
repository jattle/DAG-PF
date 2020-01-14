// File Name: phase.h
// Author: jattlelin
// Created Time: 2019-10-27 17:36:09
// Description: Phase抽象定义
// Phase实现类由工具生成，使用者可以只关注
// 流程完成后调用适当的NotifyXXX

#ifndef SRC_PHASE_H_
#define SRC_PHASE_H_

#include <atomic>
#include <map>
#include <memory>
#include <string>

#include "yapf/base/phase_common.h"
#include "yapf/base/phase_context.h"

namespace yapf {

class Phase {
 public:
  Phase() { signal_promise_ptr_ = std::make_unique<PromiseWrapper<int>>(); }
  virtual ~Phase() {}
  virtual void Initialize() {}
  void SetName(const std::string &name) { phase_name_ = name; }
  const std::string &GetName() const { return phase_name_; }
  int GetRedoRetryTimes() {
    return redo_retry_times_.load(std::memory_order_relaxed);
  }
  FutureWrapper<int> Run(PhaseContextPtr context_ptr,
                         const PhaseParamDetail &detail) {
    RedoReset();
    DoProcess(context_ptr, detail);
    return signal_promise_ptr_->GetFuture();
  }

 protected:
  virtual int DoProcess(PhaseContextPtr context_ptr,
                        const PhaseParamDetail &detail) = 0;
  // 当流程正常结束时，通知调度器
  int NotifyDone(int ret) {
    if (!signal_promise_ptr_->GetFuture().IsDone()) {
      signal_promise_ptr_->SetValue(ret);
    }
    return ret;
  }
  // 中断所有中间流程执行直接跳转到EndPhase
  int SigInterrupt() { return NotifyDone(kPhaseProcessingRetInterrupt); }
  // 跳过某阶段执行
  int NotifySkip() { return NotifyDone(kPhaseProcessingRetSkip); }
  // 通知重做本逻辑
  int NotifyRedo() {
    redo_retry_times_.fetch_add(1, std::memory_order_relaxed);
    return NotifyDone(kPhaseProcessingRetRedo);
  }
  // TODO (jattlelin) notify execption

 public:
  int NotifyTimeout() { return NotifyDone(kPhaseProcessingRetTimeout); }

 private:
  void RedoReset() {
    if (signal_promise_ptr_->GetFuture().IsDone() and
        signal_promise_ptr_->GetFuture().GetValue() ==
            kPhaseProcessingRetRedo) {
      signal_promise_ptr_.reset(new PromiseWrapper<int>);
    }
  }

 private:
  // 用于流程控制的信号量，对其设置值表示本阶段完成
  std::unique_ptr<PromiseWrapper<int>> signal_promise_ptr_;

 private:
  std::string phase_name_;
  std::atomic<int> redo_retry_times_{0};
};

using PhasePtr = std::shared_ptr<Phase>;

}  // namespace yapf

#endif  // SRC_PHASE_H_
