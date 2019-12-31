// File Name: phase_scheduler.h
// Author: jattlelin
// Created Time: 2019-12-12 15:52:38
// Description: Phase调度器定义
//
//
// TODO (jattlelin) 超时逻辑重新设计

#ifndef PHASE_SCHEDULER_H_
#define PHASE_SCHEDULER_H_

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "yapf/base/class_register.h"
#include "yapf/base/dag_processing.h"
#include "yapf/base/phase.h"
#include "yapf/base/phase_common.h"
#include "yapf/base/phase_context.h"
#include "yapf/base/scheduler_thread_pool.h"
#include "yapf/base/timer_thread.h"

namespace yapf {

enum PhaseSchedulerRet {
  kPhaseSchedulerRetParamInvalid = 80100,
  kPhaseSchedulerRetInvalidDAG,
  kPhaseSchedulerRetDAGNotBuilt,
  kPhaseSchedulerRetDAGInvalidCopy,
  kPhaseSchedulerRetHasInvalidPhase,
  kPhaseSchedulerRetNoReadyPhase,
  kPhaseSchedulerRetCreatePhaseFailed,
};

struct SchedulerOption {
  bool enable_statis{true};
  bool enable_thread_pool{true};
  bool enable_timer{true};
  bool enable_timeout{false};
  SchedulerThreadPoolOption pool_option;
};

// timeout logic context
struct NodeTimeoutContext {
  int DoTimeout();
  void AfterTimeout();

  size_t run_id{};
  PhasePtr phase_ptr;
  DAGNodePtr node;
  int timeout{};
  PhaseContextPtr ctx_ptr;
};

// redo logic context
struct NodeRedoContext : public std::enable_shared_from_this<NodeRedoContext> {
  explicit NodeRedoContext(int retry_times, int retry_interval)
      : max_retry_times(retry_times), retry_interval(retry_interval) {}
  int RedoCallback();
  void Redo(PhasePtr, PhaseContextPtr, DAGNodePtr);
  void Redo2();

  size_t run_id{};
  PhasePtr phase_ptr;
  PhaseContextPtr ctx_ptr;
  DAGNodePtr node;
  int max_retry_times{};
  int retry_interval{};
  std::function<void(PhasePtr, PhaseContextPtr, DAGNodePtr)> redo_scheduler_fn;
};

class PhaseScheduler {
 public:
  PhaseScheduler() = default;
  ~PhaseScheduler() = default;
  // 一次性启动，不可重用scheduler
  // 完整名称方式启动调度器
  int Start(const std::vector<std::pair<std::string, std::string>> &edges,
            const std::vector<std::string> &single_nodes,
            PhaseContextPtr context_ptr);
  // 别名方式启动调度器
  int Start(
      const std::vector<std::pair<std::string, std::string>> &edges,
      const std::vector<std::string> &single_nodes,
      const std::unordered_map<std::string, std::string> &node_alias_name_map,
      PhaseContextPtr context_ptr);

  // 以下方法联合使用，可以支持scheduler重用
  int BuildDAG(
      const std::vector<std::pair<std::string, std::string>> &edges,
      const std::vector<std::string> &single_nodes,
      const std::unordered_map<std::string, std::string> &node_alias_name_map);
  int CopyFrom(const PhaseScheduler &source);
  int Start(PhaseContextPtr context_ptr);
  void Clear();

  // 全局初始化
  static int GlobalInit(const SchedulerOption &option);
  // 全局销毁
  static void GlobalDestroy();

 private:
  PhaseScheduler(const PhaseScheduler &rhs);
  PhaseScheduler &operator=(const PhaseScheduler &rhs);
  int PreAllocateRes();
  int PreAllocatePhases();
  int ParsePhaseParam(DAGNodePtr node);
  int PreAllocatePhase(DAGNodePtr node);
  int ScheduleCB(PhaseContextPtr, const DAGNodePtr node,
                 const FutureWrapper<int> &);
  int ScheduleChildren(DAGNodePtr parent, PhaseContextPtr);
  int Schedule(const std::vector<DAGNodePtr> &top_nodes, PhaseContextPtr);
  int UpdateStatis(DAGNodePtr node, const FutureWrapper<int> &);
  std::string GetPhaseRetDescription(uint32_t id);
  int ReportStatis(PhaseContextPtr);

  void RunPhaseJob(PhasePtr, PhaseContextPtr, const PhaseParamDetail &,
                   DAGNodePtr);

  void RunPhaseJobThin(PhasePtr, PhaseContextPtr,
                       const PhaseParamDetail &detail, DAGNodePtr node);

  int ClearTimer(std::shared_ptr<NodeTimeoutContext> ctx,
                 const FutureWrapper<int> &ret);

  int ScheduleRedoCB(std::shared_ptr<NodeRedoContext> redoCtx,
                     const FutureWrapper<int> &last_phase_ret);

  static void InitSchedulerThreadPool(const SchedulerOption &);

 private:
  // TODO support clear
  DAG dag_;                                          // 底层的Phase关系图
  bool is_DAG_built_{false};                         //
  bool has_started_{false};                          //
  std::vector<DAGNodePtr> topology_array_;           // 保存调度结果
  std::atomic<int> schedule_cursor_{0};              // 调度顺序
  std::vector<FutureWrapper<int>> phase_ret_array_;  // 记录每个阶段的返回值
  std::vector<int64_t> phase_timecost_array_;    // 记录每个阶段的耗时
  std::atomic<bool> is_sig_interrupted_{false};  // 中断标记
  std::atomic<int> ir_reason_{0};                // 中断原因
  std::vector<PhasePtr> phase_pool_;             // Phase存储池
  std::vector<PhaseParamDetail> phase_param_pool_;  // Phase静态参数池(可共享)
  std::vector<PhaseParamDetail> *phase_param_pool_ptr_{
      nullptr};                                // 共享指针，避免复制
  inline static bool s_enable_statis_{false};  // 是否打印统计数据日志(全局开关)
  inline static bool s_verbose_{false};  // 是否输出详细信息
  inline static bool s_enable_thread_pool_{
      false};  // 是否使用线程池并发调度Phase
  inline static bool s_enable_timer_thread_{false};  // 是否使用超时队列
  inline static bool s_enable_timeout_check_{false};  // 是否启用Phase超时检查
  inline static std::atomic<size_t> s_run_id_{0};
  // 调度线程池相关
  inline static bool s_is_global_inited_{false};
  inline static SchedulerThreadPool s_cb_thread_pool_;
  inline static TimerThread s_timer_thread_;
  friend class NodeTimeoutContext;
  friend class NodeRedoContext;
};

///
/// phase scheduler helper functions
///

// reusedScheduler: 从已初始化的scheduler复制一份，避免每次重复构建DAG
int StartScheduler(const PhaseScheduler &reused_scheduler,
                   PhaseContextPtr context_ptr);

// 预分配并初始化一个scheduler，后续可以重用减少开销
int InitSchedulers(
    const std::vector<std::string> &exprs,
    const std::unordered_map<std::string, std::string> &phase_class_map,
    PhaseScheduler &reused_scheduler);

};  // namespace yapf
#endif

