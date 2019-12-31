// File Name: phase_scheduler.cpp
// Author: jattlelin
// Created Time: 2019-12-12 17:33:14
// Description: Phase自动调度的实现
// 根据依赖关系建立DAG图，输出拓扑排序执行计划
// 根据执行计划调度各阶段串行或并发执行

#include "yapf/base/phase_scheduler.h"

#include "logging.h"
#include "yapf/flow_control/FlowControlFactory.h"

#ifndef __PHASE_NAMESPACE
#define __PHASE_NAMESPACE ""
#endif

namespace yapf {

PhaseContext::~PhaseContext() {
  DAGPF_LOG_INFO << "destroy context..." << std::endl;
  delete scheduler_ptr;
}

static bool HasRegistered(const std::string &full_name) {
  std::string class_name = full_name.substr(0, full_name.find("("));
  DAGPF_LOG_INFO << "class_name: " << class_name << std::endl;
  return HasRegisted<yapf::Phase>(__PHASE_NAMESPACE, class_name);
}

int PhaseScheduler::Start(
    const std::vector<std::pair<std::string, std::string>> &edges,
    const std::vector<std::string> &single_nodes, PhaseContextPtr context_ptr) {
  if (edges.empty() && single_nodes.empty()) {
    return kPhaseSchedulerRetParamInvalid;
  }
  std::unordered_map<std::string, std::string> node_alias_map;
  int ret = BuildDAG(edges, single_nodes, node_alias_map);
  if (ret != 0) return ret;
  // preallocate phase
  ret = PreAllocatePhases();
  if (ret != 0) {
    DAGPF_LOG_ERROR << "preAllocate phase failed." << std::endl;
    return ret;
  }
  std::vector<DAGNodePtr> nodes(1, dag_.GetStartNode());
  return Schedule(nodes, context_ptr);
}

int PhaseScheduler::Start(
    const std::vector<std::pair<std::string, std::string>> &edges,
    const std::vector<std::string> &single_nodes,
    const std::unordered_map<std::string, std::string> &node_alias_map,
    PhaseContextPtr context_ptr) {
  if ((edges.empty() && single_nodes.empty()) || node_alias_map.empty()) {
    return kPhaseSchedulerRetParamInvalid;
  }
  int ret = BuildDAG(edges, single_nodes, node_alias_map);
  if (ret != 0) return ret;
  // preallocate phase
  ret = PreAllocatePhases();
  if (ret != 0) {
    DAGPF_LOG_ERROR << "preAllocate phase failed." << std::endl;
    return ret;
  }
  std::vector<DAGNodePtr> nodes(1, dag_.GetStartNode());
  return Schedule(nodes, context_ptr);
}

int PhaseScheduler::CopyFrom(const PhaseScheduler &source) {
  if (!source.is_DAG_built_ || source.has_started_) {
    DAGPF_LOG_ERROR << "invalid scheduler, cant copy. "
                    << " is_DAG_built_ = " << source.is_DAG_built_
                    << ", has_started_ = " << source.has_started_ << std::endl;
    return kPhaseSchedulerRetDAGInvalidCopy;
  }
  // TODO
  int ret = this->dag_.CopyFrom(source.dag_);
  if (ret != 0) {
    return ret;
  }
  this->is_DAG_built_ = source.is_DAG_built_;
  this->has_started_ = source.has_started_;
  this->phase_pool_ = source.phase_pool_;
  // this->phase_param_pool_ = source.phase_param_pool_;
  this->phase_param_pool_ptr_ = source.phase_param_pool_ptr_;
  this->phase_ret_array_ = source.phase_ret_array_;
  this->topology_array_ = source.topology_array_;
  this->phase_timecost_array_ = source.phase_timecost_array_;
  return 0;
}

int PhaseScheduler::BuildDAG(
    const std::vector<std::pair<std::string, std::string>> &edges,
    const std::vector<std::string> &single_nodes,
    const std::unordered_map<std::string, std::string> &node_alias_name_map) {
  int ret = dag_.AddNodeLinks(edges, single_nodes, node_alias_name_map);
  if (ret != 0) {
    DAGPF_LOG_ERROR << "add node links failed: ret = " << ret << std::endl;
    return kPhaseSchedulerRetInvalidDAG;
  }
  ret = dag_.Init(&HasRegistered);
  if (ret != 0) {
    DAGPF_LOG_ERROR << "init DAG failed: ret = " << ret << std::endl;
    return kPhaseSchedulerRetInvalidDAG;
  }
  DAGPF_LOG_DEBUG << "topology sort node list:" << std::endl;
  // TODO (jattlelin) check if needed
  if (s_verbose_) {
    dag_.List();
  }
  is_DAG_built_ = true;
  ret = PreAllocateRes();
  if (ret != 0) return ret;
  return 0;
}

int PhaseScheduler::Start(PhaseContextPtr context_ptr) {
  if (!is_DAG_built_) {
    DAGPF_LOG_ERROR << "DAG is not built." << std::endl;
    return kPhaseSchedulerRetDAGNotBuilt;
  }
  DAGPF_LOG_INFO << "preAllocate phases." << std::endl;
  // preallocate phase
  int ret = PreAllocatePhases();
  if (ret != 0) {
    DAGPF_LOG_ERROR << "preAllocate phase failed." << std::endl;
    return ret;
  }
  std::vector<DAGNodePtr> nodes(1, dag_.GetStartNode());
  return Schedule(nodes, context_ptr);
}

int PhaseScheduler::ParsePhaseParam(DAGNodePtr node) {
  DAGPF_LOG_DEBUG << "parse phase param: " << node->GetName() << std::endl;
  phase_param_pool_[node->GetId()].config_key.Parse(node->GetFullName());
  return 0;
}

int PhaseScheduler::PreAllocatePhase(DAGNodePtr node) {
  const std::string &name =
      (*phase_param_pool_ptr_)[node->GetId()].config_key.name;
  std::shared_ptr<Phase> phase_ptr(
      CreateObject<Phase>(__PHASE_NAMESPACE, name));
  if (not phase_ptr) {
    DAGPF_LOG_ERROR << "cant create phase instance: " << name
                    << ", full name: " << node->GetFullName() << std::endl;
    return kPhaseSchedulerRetCreatePhaseFailed;
  }
  phase_pool_[node->GetId()] = phase_ptr;
  return 0;
}

int PhaseScheduler::PreAllocatePhases() {
  auto functor =
      std::bind(&PhaseScheduler::PreAllocatePhase, this, std::placeholders::_1);
  int ret = dag_.TraverseAction(functor);
  if (ret != 0) return ret;
  return 0;
}

int PhaseScheduler::PreAllocateRes() {
  //在DAG构建后预先分配存储
  static const FutureWrapper<int> default_ret;
  phase_ret_array_.resize(dag_.Size(), default_ret);
  phase_param_pool_.resize(dag_.Size());
  topology_array_.resize(dag_.Size(), nullptr);
  phase_timecost_array_.resize(dag_.Size(), 0);
  phase_pool_.resize(dag_.Size(), PhasePtr());
  auto functor =
      std::bind(&PhaseScheduler::ParsePhaseParam, this, std::placeholders::_1);
  int ret = dag_.TraverseAction(functor);
  if (ret != 0) {
    DAGPF_LOG_ERROR << "traverse action failed, ret = " << ret << std::endl;
    return ret;
  }
  phase_param_pool_ptr_ = &phase_param_pool_;
  return 0;
}

int PhaseScheduler::ScheduleChildren(DAGNodePtr parent,
                                     PhaseContextPtr context_ptr) {
  std::vector<DAGNodePtr> nodes;
  // pop ready children nodes
  int ret = dag_.Pop(parent, nodes);
  if (ret != 0) {
    DAGPF_LOG_DEBUG << "pop failed. parent name: " << parent->GetName()
                    << ", children nodes size: " << nodes.size()
                    << ", ret = " << ret << std::endl;
    return kPhaseSchedulerRetNoReadyPhase;
  }
  return Schedule(nodes, context_ptr);
}

int PhaseScheduler::Schedule(const std::vector<DAGNodePtr> &nodes,
                             PhaseContextPtr context_ptr) {
  DAGPF_LOG_INFO << "schedule phases. nodes size: " << nodes.size()
                 << std::endl;
  for (const auto &node : nodes) {
    DAGPF_LOG_DEBUG << "schedule phase: " << node->GetName() << std::endl;
    // TODO parse phase param detail
    auto &phase_ptr = phase_pool_[node->GetId()];
    phase_ptr->SetName(node->GetName());
    DAGPF_LOG_DEBUG << "prepare to launch phase: " << node->GetName()
                    << ", timestamp: " << Utils::getNowMs() << std::endl;
    if (s_enable_statis_) {
      // record start time
      phase_timecost_array_[node->GetId()] = Utils::getNowMs();
    }
    if (is_sig_interrupted_.load(std::memory_order_relaxed) && node != dag_.GetEndNode()) {
      // skip running phase other than EndPhase if scheduler has been
      // interrupted
      FutureWrapper<int> ret;
      //PromiseWrapper<int> promise_ret;
      PromiseWrapper<int> promise_ret{true};
      promise_ret.SetValue(kPhaseProcessingRetSkip);
      ret = promise_ret.GetFuture();
      ret.Then(std::bind(&PhaseScheduler::ScheduleCB, this, context_ptr, node,
                         std::placeholders::_1));
    } else {
      // if coroutine enabled or thread pool enabled, submit job to thread pool
      if (s_enable_thread_pool_) {
        JobClosure jc = std::bind(
            &PhaseScheduler::RunPhaseJob, this, phase_ptr, context_ptr,
            (*phase_param_pool_ptr_)[node->GetId()], node);
        s_cb_thread_pool_.Submit(std::move(jc));
      } else {
        RunPhaseJob(phase_ptr, context_ptr,
                    (*phase_param_pool_ptr_)[node->GetId()], node);
      }
    }
  }
  return 0;
}

void PhaseScheduler::RunPhaseJob(PhasePtr phase_ptr, PhaseContextPtr ctx_ptr,
                                 const PhaseParamDetail &detail,
                                 DAGNodePtr node) {
  size_t run_id = s_run_id_.fetch_add(1, std::memory_order_relaxed) + 1;
  DAGPF_LOG_DEBUG << "run phase job " << phase_ptr->GetName()
                  << ", flow_control = "
                  << detail.config_key.params["flow_control"].bv << std::endl;
  FutureWrapper<int> ret;
  // PromiseWrapper<int> promise_ret;
  PromiseWrapper<int> promise_ret{true};
  do {
    // check flow control
    if (detail.config_key.params["flow_control"].bv) {
      size_t flow_win_size = detail.config_key.params["flow_win_size"].iv;
      size_t flow_limit = detail.config_key.params["flow_limit"].iv;
      bool delay = detail.config_key.params["flow_limit_delay"].bv;
      int delay_timeout = detail.config_key.params["delay_timeout"].iv;
      static constexpr size_t kDelayTimeout = 5 * 1000;
      auto flow_controller =
          FlowControlFactory::getInstance()->getFlowController(
              node->GetFullName(), flow_win_size, flow_limit);
      if (flow_controller->rateLimited()) {
        DAGPF_LOG_DEBUG << "flow limited." << std::endl;
        if (!delay) {
          promise_ret.SetValue(kPhaseProcessingRetFlowLimited);
          ret = promise_ret.GetFuture();
          break;
        } else {
          DAGPF_LOG_DEBUG << "submit delay task." << std::endl;
          // submit delay task
          if (delay_timeout == 0) delay_timeout = kDelayTimeout;
          flow_controller->delay2(
              run_id, delay_timeout,
              [phase_ptr, this, ctx_ptr, node](long id, size_t timeout) {
                JobClosure jc = std::bind([this, ctx_ptr, node]() {
                  FutureWrapper<int> ret;
                  // PromiseWrapper<int> promise_ret;
                  PromiseWrapper<int> promise_ret{true};
                  promise_ret.SetValue(kPhaseProcessingRetDelayTimeout);
                  ret = promise_ret.GetFuture();
                  ret.Then(std::bind(&PhaseScheduler::ScheduleCB, this, ctx_ptr,
                                     node, std::placeholders::_1));
                });
                PhaseScheduler::s_cb_thread_pool_.Submit(std::move(jc));
              },
              &PhaseScheduler::RunPhaseJobThin, this, phase_ptr, ctx_ptr,
              detail, node);
          return;
        }
      }
    }
  } while (0);
  if (ret.IsDone()) {
    // flow limited
    ret.Then(std::bind(&PhaseScheduler::ScheduleCB, this, ctx_ptr, node,
                       std::placeholders::_1));
  } else {
    RunPhaseJobThin(phase_ptr, ctx_ptr, detail, node);
  }
}

void PhaseScheduler::RunPhaseJobThin(PhasePtr phase_ptr,
                                     PhaseContextPtr ctx_ptr,
                                     const PhaseParamDetail &detail,
                                     DAGNodePtr node) {
  DAGPF_LOG_DEBUG << "run phase job without other top level logic: "
                  << phase_ptr->GetName() << std::endl;
  FutureWrapper<int> ret;
  // PromiseWrapper<int> promise_ret;
  PromiseWrapper<int> promise_ret{true};
  do {
    try {
      ret = phase_ptr->Run(ctx_ptr, detail);
      break;
    } catch (std::exception &ex) {
      DAGPF_LOG_ERROR << "run phase: " << phase_ptr->GetName()
                      << " catch exception: " << ex.what() << std::endl;
    } catch (...) {
      DAGPF_LOG_ERROR << "run phase: " << phase_ptr->GetName()
                      << " catch unknown exception: " << std::endl;
    }
    promise_ret.SetValue(kPhaseProcessingRetSkip);
    ret = promise_ret.GetFuture();
  } while (0);
  if (ret.IsDone()) {
    DAGPF_LOG_DEBUG << "ret is Done, value = " << ret.GetValue() << std::endl;
  }
  // redo logic
  static constexpr size_t kRedoDefaultRetryTimes = 3;
  static constexpr size_t kRedoDefaultRetryInterval =
      1000;  // retry every 500ms
  if (detail.config_key.params["redo"].bv and s_enable_thread_pool_) {
    do {
      if (ret.IsDone() and ret.GetValue() != kPhaseProcessingRetRedo) break;
      int max_retry_times = detail.config_key.params["redo_retry_times"].iv;
      if (max_retry_times == 0) {
        max_retry_times = kRedoDefaultRetryTimes;
      }
      int retry_interval = detail.config_key.params["redo_retry_interval"].iv;
      if (retry_interval == 0) {
        retry_interval = kRedoDefaultRetryInterval;
      }
      auto redo_ctx =
          std::make_shared<NodeRedoContext>(max_retry_times, retry_interval);
      size_t run_id = s_run_id_.fetch_add(std::memory_order_relaxed) + 1;
      redo_ctx->run_id = run_id;
      redo_ctx->phase_ptr = phase_ptr;
      redo_ctx->node = node;
      redo_ctx->ctx_ptr = ctx_ptr;
      using std::placeholders::_1;
      using std::placeholders::_2;
      using std::placeholders::_3;
      redo_ctx->redo_scheduler_fn =
          std::bind(&PhaseScheduler::RunPhaseJobThin, this, _1, _2, detail, _3);
      DAGPF_LOG_DEBUG << "set redo. phase_name: " << phase_ptr->GetName()
                      << ", retry_times: "
                      << redo_ctx->phase_ptr->GetRedoRetryTimes()
                      << ", max_retry_imes: " << redo_ctx->max_retry_times
                      << ", retry_interval: " << retry_interval << std::endl;
      ret.Then(std::bind(&PhaseScheduler::ScheduleRedoCB, this, redo_ctx,
                         std::placeholders::_1));
      return;
    } while (0);
  }
  ret.Then(std::bind(&PhaseScheduler::ScheduleCB, this, ctx_ptr, node,
                     std::placeholders::_1));
}

int PhaseScheduler::ScheduleRedoCB(std::shared_ptr<NodeRedoContext> redo_ctx,
                                   const FutureWrapper<int> &last_phase_ret) {
  DAGPF_LOG_DEBUG << "phase_name: " << redo_ctx->phase_ptr->GetName()
                  << ", max_retry_times: " << redo_ctx->max_retry_times
                  << ", phase retry_times: "
                  << redo_ctx->phase_ptr->GetRedoRetryTimes() << std::endl;
  // if need redo
  if (redo_ctx->node != dag_.GetEndNode() and last_phase_ret.IsDone() and
      last_phase_ret.GetValue() == kPhaseProcessingRetRedo) {
    int retry_times = redo_ctx->phase_ptr->GetRedoRetryTimes();
    if (retry_times > redo_ctx->max_retry_times) {
      DAGPF_LOG_DEBUG << "max retry limit, phase_name: "
                      << redo_ctx->node->GetName() << std::endl;
      FutureWrapper<int> phase_ret;
      // PromiseWrapper<int> promise_ret;
      PromiseWrapper<int> promise_ret{true};
      promise_ret.SetValue(kPhaseProcessingRetMaxRetry);
      phase_ret = promise_ret.GetFuture();
      return this->ScheduleCB(redo_ctx->ctx_ptr, redo_ctx->node, phase_ret);
    }
    DAGPF_LOG_DEBUG << "submit redo timer callback, phase_name: "
                    << redo_ctx->node->GetName() << std::endl;
    // submit redo timer callback
    return s_timer_thread_.push(
        redo_ctx->run_id, std::bind(&NodeRedoContext::RedoCallback, redo_ctx),
        redo_ctx->retry_interval);
  } else {
    return this->ScheduleCB(redo_ctx->ctx_ptr, redo_ctx->node, last_phase_ret);
  }
}

int PhaseScheduler::UpdateStatis(DAGNodePtr node,
                                 const FutureWrapper<int> &last_phase_ret) {
  // record phase ret
  if (!s_enable_statis_) return 0;
  // record scheduler path
  // topology_array_[schedule_cursor_++] = node;
  topology_array_[schedule_cursor_.fetch_add(1, std::memory_order_relaxed)] = node;
  // calculate timecost
  phase_timecost_array_[node->GetId()] =
      Utils::getNowMs() - phase_timecost_array_[node->GetId()];
  return 0;
}

std::string PhaseScheduler::GetPhaseRetDescription(uint32_t id) {
  std::string ret_str;
  const FutureWrapper<int> &ret = phase_ret_array_[id];
  if (ret.IsDone()) {
    ret_str.append("ret:").append(std::to_string(ret.GetValue()));
  } else {
    ret_str.append("ret: None.");
  }
  return ret_str;
}

//打印统计日志:包括业务自定义日志、每阶段耗时及返回值
int PhaseScheduler::ReportStatis(PhaseContextPtr ctx_ptr) {
  if (!s_enable_statis_ || !ctx_ptr->log_switch) {
    return 0;
  }
  //业务自定义log部分
  const std::string &str_head = ctx_ptr->GetLogHead();
  std::string str_procedure_statis;
  for (auto iter = topology_array_.begin(); iter != topology_array_.end();
       ++iter) {
    int64_t timecost = phase_timecost_array_[(*iter)->GetId()];
    std::string desc = GetPhaseRetDescription((*iter)->GetId());
    if (!str_head.empty()) {
      str_procedure_statis.append("|");
    } else if (iter != topology_array_.begin()) {
      str_procedure_statis.append("|");
    }
    str_procedure_statis.append((*iter)->GetName())
        .append("(phase_ret[")
        .append(desc)
        .append("],timecost[")
        .append(std::to_string(timecost))
        .append("])");
  }
  DAGPF_LOG_DEBUG << "report statis." << std::endl;
  str_procedure_statis.append("|total_timecost:")
      .append(std::to_string(Utils::getNowMs() - ctx_ptr->create_time_ms));
  std::string log_content = str_head;
  log_content.append(str_procedure_statis);
  // TODO logging
  DAGPF_LOG_DEBUG << "phase_statis|" << log_content << std::endl;
  for (const auto &handler : ctx_ptr->log_export_handlers) {
    if (handler) {
      handler(log_content);
    }
  }
  return 0;
}

/// phase执行完毕后的回调
int PhaseScheduler::ScheduleCB(PhaseContextPtr ctx_ptr, DAGNodePtr node,
                               const FutureWrapper<int> &last_phase_ret) {
  DAGPF_LOG_DEBUG << "cb return of phase: " << node->GetName()
                  << ", timestamp: " << Utils::getNowMs() << std::endl;
  //记录返回值
  phase_ret_array_[node->GetId()] = last_phase_ret;
  UpdateStatis(node, last_phase_ret);
  if (node != dag_.GetEndNode() && last_phase_ret.IsDone() &&
      (last_phase_ret.GetValue() == kPhaseProcessingRetInterrupt ||
       last_phase_ret.GetValue() == kPhaseProcessingRetFlowLimited) &&
      !is_sig_interrupted_.load(std::memory_order_acquire)) {
    //设置中断标记
    ir_reason_.store(last_phase_ret.GetValue(), std::memory_order_relaxed);
    is_sig_interrupted_.store(true, std::memory_order_release);
  }
  // last phase
  if (node == dag_.GetEndNode()) {
    ctx_ptr->is_interrupted = is_sig_interrupted_.load(std::memory_order_relaxed);
    ctx_ptr->ir_reason = ir_reason_.load(std::memory_order_relaxed);
    ReportStatis(ctx_ptr);
    return 0;
  }
  return ScheduleChildren(node, ctx_ptr);
}

void PhaseScheduler::InitSchedulerThreadPool(const SchedulerOption &option) {
  s_enable_statis_ = option.enable_statis;
  s_enable_thread_pool_ = option.enable_thread_pool;
  s_enable_timer_thread_ = option.enable_timer;
  if (s_enable_thread_pool_) {
    int ret = s_cb_thread_pool_.Init(option.pool_option);
    if (ret != 0) {
      s_enable_thread_pool_ = false;
      return;
    }
    ret = s_cb_thread_pool_.Start();
    if (ret != 0) {
      s_enable_thread_pool_ = false;
      return;
    }
  }
  if (s_enable_timer_thread_ && s_enable_thread_pool_) {
    s_timer_thread_.start();
  }
  s_enable_timeout_check_ = option.enable_timeout;
}

void PhaseScheduler::Clear() {
  dag_.Clear();
  is_DAG_built_ = false;
  has_started_ = false;
  topology_array_.clear();
  schedule_cursor_.store(0, std::memory_order_relaxed);
  phase_ret_array_.clear();
  phase_timecost_array_.clear();
  is_sig_interrupted_.store(false, std::memory_order_relaxed);
  ir_reason_.store(0, std::memory_order_relaxed);
  phase_pool_.clear();
  phase_param_pool_.clear();
  phase_param_pool_ptr_ = nullptr;
}

static std::once_flag init_scheduler_once;

int PhaseScheduler::GlobalInit(const SchedulerOption &option) {
  std::call_once(init_scheduler_once, &PhaseScheduler::InitSchedulerThreadPool,
                 option);
  s_is_global_inited_ = true;
  return 0;
}

void PhaseScheduler::GlobalDestroy() { s_cb_thread_pool_.Stop(); }

int NodeTimeoutContext::DoTimeout() {
  // phase timeout
  JobClosure jc = std::bind(&NodeTimeoutContext::AfterTimeout, this);
  PhaseScheduler::s_cb_thread_pool_.Submit(std::move(jc));
  return 0;
}

void NodeTimeoutContext::AfterTimeout() {
  DAGPF_LOG_DEBUG << "phase timeout, name = " << node->GetName()
                  << ", full name = " << node->GetFullName()
                  << ", run_id = " << run_id << ", timeout = " << timeout
                  << std::endl;
  phase_ptr->NotifyTimeout();
}

int NodeRedoContext::RedoCallback() {
  // phase timeout
  DAGPF_LOG_DEBUG << "phase timeout. redo now." << std::endl;
  JobClosure jc = std::bind(&NodeRedoContext::Redo2, shared_from_this());
  PhaseScheduler::s_cb_thread_pool_.Submit(std::move(jc));
  return 0;
}

void NodeRedoContext::Redo(PhasePtr phase_ptr, PhaseContextPtr ctx_ptr,
                           DAGNodePtr node) {
  DAGPF_LOG_DEBUG << "redo phase, name = " << node->GetName()
                  << ", full name = " << node->GetFullName()
                  << ", runId = " << run_id << std::endl;
  this->redo_scheduler_fn(phase_ptr, ctx_ptr, node);
}

void NodeRedoContext::Redo2() {
  DAGPF_LOG_DEBUG << "redo phase, name = " << node->GetName()
                  << ", full name = " << node->GetFullName()
                  << ", runId = " << run_id << std::endl;
  this->redo_scheduler_fn(this->phase_ptr, this->ctx_ptr, this->node);
}

int PhaseScheduler::ClearTimer(std::shared_ptr<NodeTimeoutContext> ctx,
                               const FutureWrapper<int> &ret) {
  // normal phase terminate
  int erase_ret = s_timer_thread_.erase(ctx->run_id);
  DAGPF_LOG_DEBUG << "clear timer."
                  << ", full name = " << ctx->node->GetFullName()
                  << ", runId = " << ctx->run_id
                  << ", timeout = " << ctx->timeout << std::endl;
  if (erase_ret == 0) {
    DAGPF_LOG_DEBUG << "erase success." << std::endl;
  }
  return 0;
}

/////////////////////////////// Helper Functions
///////////////////////////////////////////////////////

// reusedScheduler: 从已初始化的scheduler复制一份，避免每次重复构建DAG
int StartScheduler(const PhaseScheduler &reused_scheduler,
                   PhaseContextPtr context_ptr) {
  context_ptr->scheduler_ptr = new PhaseScheduler();
  int ret = context_ptr->scheduler_ptr->CopyFrom(reused_scheduler);
  if (ret != 0) {
    DAGPF_LOG_ERROR << "copy scheduler failed." << std::endl;
    return ret;
  }
  DAGPF_LOG_INFO << "copy scheduler success." << std::endl;
  context_ptr->create_time_ms = Utils::getNowMs();
  return context_ptr->scheduler_ptr->Start(context_ptr);
}

int InitSchedulers(
    const std::vector<std::string> &exprs,
    const std::unordered_map<std::string, std::string> &phase_class_map,
    PhaseScheduler &reused_scheduler) {
  // parse links
  std::vector<std::pair<std::string, std::string>> links;
  std::vector<std::string> single_nodes;
  int ret = ParseExprs(exprs, links, single_nodes);
  if (ret != 0) {
    DAGPF_LOG_ERROR << "parse exprs failed." << std::endl;
    return -1;
  }
  ret = reused_scheduler.BuildDAG(links, single_nodes, phase_class_map);
  if (ret != 0) {
    DAGPF_LOG_ERROR << "build DAG failed." << std::endl;
    return -2;
  }
  return 0;
}

}  // namespace yapf

