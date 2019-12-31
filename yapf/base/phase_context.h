// File Name: phase_context.h
// Author: jattlelin
// Created Time: 2019-12-24 20:02:54
// Description:

#ifndef PHASE_CONTEXT_H_
#define PHASE_CONTEXT_H_

#include <functional>
#include <string>
#include <vector>

namespace yapf {

class PhaseScheduler;
struct PhaseContext {
  PhaseContext(const PhaseContext&) = delete;
  PhaseContext& operator=(const PhaseContext&) = delete;

  PhaseContext() = default;
  virtual ~PhaseContext();

  // loghead，scheduler打印日志时调用
  virtual std::string GetLogHead() const {
    static const std::string kEmpty;
    return kEmpty;
  }
  // 业务自行定义，用于区分具体的session类型
  virtual int GetCtxType() const { return 0; }

  int AddLogHandler(std::function<void(const std::string&)> handler) {
    if (handler) {
      log_export_handlers.emplace_back(std::move(handler));
    }
    return 0;
  }

  void SetLogSwitch(bool flag) { log_switch = flag; }

  int64_t create_time_ms{0};  // 创建时间戳(ms)
  bool log_switch{true};      // 是否打印本会话的统计日志
  bool is_interrupted{false};
  int ir_reason{0};  // interrupted reason
  std::vector<std::function<void(const std::string&)>> log_export_handlers;
  PhaseScheduler *scheduler_ptr{nullptr};
};

using PhaseContextPtr = std::shared_ptr<PhaseContext>;

template <typename BizCtxType>
std::shared_ptr<BizCtxType> ToBizCtxPtr(PhaseContextPtr ctx_ptr) {
  return std::static_pointer_cast<BizCtxType>(ctx_ptr);
}

//template <typename BizCtxType>
//auto ToBizCtxPtr(PhaseContextPtr ctx_ptr) {
//  return static_cast<BizCtxType*>(ctx_ptr.get());
//}

}  // namespace yapf

#endif

