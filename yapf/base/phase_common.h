// File Name: phase_common.h
// Author: jattlelin
// Created Time: 2019-10-27 21:41:12
// Description: 通用定义
// 包括PhaseContext, Future的封装，一些工具类函数

#ifndef SRC_PHASE_COMMON_H_
#define SRC_PHASE_COMMON_H_

#include <atomic>
#include <cassert>
#include <functional>
#include <future>
#include <iterator>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace yapf {

//
// phase retcode:
// 80000 - 85000 为内部保留错误码
enum EPhaseProcessingRet {
  kPhaseProcessingRetOk = 0,
  kPhaseProcessingRetInterrupt = 84000,  // 中断，调度器直接跳转到收尾阶段
  kPhaseProcessingRetSkip,               // 跳过此阶段001
  kPhaseProcessingRetTimeout,            // 此阶段超时002
  kPhaseProcessingRetFlowLimited,            // 此阶段命中流控003
  kPhaseProcessingRetDelayTimeout,           // 此阶段延迟提交超时004
  kPhaseProcessingDepPhaseRetPartialFailed,  // 部分依赖失败
  kPhaseProcessingDepPhaseRetAllFailed,      // 全部依赖失败
  kPhaseProcessingRetException,              //
  kPhaseProcessingRetRedo,                   // 重做此阶段
  kPhaseProcessingRetMaxRetry,               // 重试次数超出限制
};

bool StrToInt64(const char* str, int64_t& value);

bool StrToDouble(const char* str, double& value);

int ParseExprs(const std::vector<std::string>& exprs,
               std::vector<std::pair<std::string, std::string>>& pairs,
               std::vector<std::string>& single_nodes,
               const std::string& sep = "->");

template <typename T>
std::string ToString(
    T v,
    typename std::enable_if<std::is_same<std::string, std::decay_t<T>>::value,
                            std::string>::type* = 0) {
  return v;
}

template <typename T>
std::string ToString(
    T v, typename std::enable_if<
             !std::is_same<std::string, std::decay_t<T>>::value>::type* = 0) {
  return std::to_string(v);
}

// 选择vector元素的某个字段
// DT: 元素类型
// FT: 需要选择的元素字段类型
template <typename DT, typename FT>
struct JoinFieldT {
  explicit JoinFieldT(const FT DT::*mp, const std::string& sep)
      : m_mp(mp), m_sep(sep) {}

  std::string operator()(const std::vector<DT>& vals) {
    std::string str_fields;
    for (auto iter = vals.begin(); iter != vals.end(); ++iter) {
      if (iter != vals.begin()) {
        str_fields.append(m_sep);
      }
      str_fields.append(ToString((*iter).*m_mp));
    }
    return str_fields;
  }
  const FT DT::*m_mp{nullptr};
  std::string m_sep;
};

template <typename DT, typename FT, typename VDT>
struct JoinPtrFieldT {
  JoinPtrFieldT() {}
  explicit JoinPtrFieldT(const FT DT::*mp, const std::string& sep)
      : m_mp(mp), m_sep(sep) {}
  explicit JoinPtrFieldT(const FT& (DT::*mfp)() const, const std::string& sep)
      : m_mfp(mfp), m_sep(sep) {}
  std::string operator()(const std::vector<VDT>& vals) {
    std::string strFields;
    for (auto iter = vals.begin(); iter != vals.end(); ++iter) {
      if (iter != vals.begin()) {
        strFields.append(m_sep);
      }
      if (m_mp) {
        strFields.append((**iter).*m_mp);
      } else if (m_mfp) {
        strFields.append(((**iter).*m_mfp)());
      } else {
        assert(false);
      }
    }
    return strFields;
  }
  const FT DT::*m_mp{nullptr};
  const FT& (DT::*m_mfp)() const {nullptr};
  std::string m_sep;
};

template <typename DT, typename FT, typename VDT = std::shared_ptr<DT>>
std::string JoinPtrField(const std::vector<VDT>& vals, const FT DT::*mp,
                         const std::string& sep) {
  JoinPtrFieldT<DT, FT, VDT> jf(mp, sep);
  return jf(vals);
}

template <typename DT, typename FT, typename VDT = std::shared_ptr<DT>>
std::string JoinPtrField(const std::vector<VDT>& vals,
                         const FT& (DT::*mfp)() const, const std::string& sep) {
  JoinPtrFieldT<DT, FT, VDT> jf(mfp, sep);
  return jf(vals);
}

template <typename DT, typename FT>
std::string JoinField(const std::vector<DT>& vals, const FT DT::*mp,
                      const std::string& sep) {
  JoinFieldT<DT, FT> jf(mp, sep);
  return jf(vals);
}

template <typename T>
std::string Join(const std::vector<T>& vals, const std::string& sep) {
  std::stringstream oss;
  std::ostream_iterator<T> os_it(oss, sep.c_str());
  std::copy(vals.begin(), vals.end(), os_it);
  std::string ret = oss.str();
  if (!ret.empty() && !sep.empty()) {
    size_t index = ret.rfind(sep);
    if (index != std::string::npos) ret.erase(index);
  }
  return ret;
}

// from hugo lib
struct PhaseConfigValue {
  std::string str;
  int64_t iv{};
  double dv{0.0};
  bool bv{false};

  bool invalid{false};
  PhaseConfigValue(bool invalid_ = false)
      : iv(0), dv(0), bv(false), invalid(invalid_) {}
  PhaseConfigValue& operator=(const std::string& v) {
    str = v;
    if (StrToInt64(v.c_str(), iv)) {
      return *this;
    }
    if (StrToDouble(v.c_str(), dv)) {
      return *this;
    }
    if (v == "true") {
      bv = true;
      return *this;
    }
    if (v == "false") {
      bv = false;
      return *this;
    }
    return *this;
  }
};

struct PhaseConfig {
  using PhaseConfigValueTable =
      std::unordered_map<std::string, PhaseConfigValue>;
  PhaseConfigValueTable params;
  const PhaseConfigValue& operator[](const std::string& name) const {
    auto it = params.find(name);
    if (it != params.end()) {
      return it->second;
    }
    static const PhaseConfigValue kDefault(true);
    return kDefault;
  }
};

std::vector<std::string_view> SepString(std::string_view sv,
                                        const std::string_view& delim);

std::string_view Trim(std::string_view str);

struct PhaseConfigKey {
  std::string name;
  PhaseConfig params;
  void Parse(std::string_view v) {
    if (v.find("(") != std::string_view::npos &&
        v.find(")") != std::string_view::npos) {
      auto svt = v.substr(0, v.find("("));
      name.assign(svt.data(), svt.size());
      auto param_values =
          v.substr(v.find("(") + 1, v.find(")") - v.find("(") - 1);
      auto ss = SepString(param_values, ",");
      for (size_t i = 0; i < ss.size(); i++) {
        auto kv = SepString(ss[i], ":");
        if (kv.size() == 2u) {
          auto svk = Trim(kv[0]);
          std::string k(svk.data(), svk.size());
          auto svv = Trim(kv[1]);
          std::string v(svv.data(), svv.size());
          params.params[k] = v;
        }
      }
    } else {
      name.assign(v.data(), v.size());
    }
  }
};

struct PhaseParamDetail {
  PhaseConfigKey config_key;  // phaseName(k:v,...)配置参数解析成的结构
};

//
// simple wrapper for promise/future
//
template <typename T>
class FutureWrapper;

template <typename T>
class FutureWrapperBase;

template <typename T>
class PromiseWrapper {
 public:
  PromiseWrapper(bool fast_forward = false) : fast_forward_(fast_forward) {
    std::future<T> f = promise_holder_.get_future();
    future_wrapper_ = FutureWrapper<T>(
        std::make_shared<FutureWrapperBase<T>>(std::move(f), fast_forward));
  }
  PromiseWrapper(const PromiseWrapper&) = delete;
  PromiseWrapper(PromiseWrapper&& other) {
    promise_holder_ = std::move(other.promise_holder_);
    future_wrapper_ = std::move(other.future_wrapper_);
  }

  FutureWrapper<T> GetFuture() { return future_wrapper_; }

  void SetValue(T t) {
    if (!fast_forward_) {
      promise_holder_.set_value(t);
    }
    future_wrapper_.Notify(t);
  }

 private:
  std::promise<T> promise_holder_;
  FutureWrapper<T> future_wrapper_;
  bool fast_forward_{false};
};

template <typename T>
class FutureWrapper {
 public:
  FutureWrapper() {}
  explicit FutureWrapper(std::shared_ptr<FutureWrapperBase<T>> ptr)
      : future_ptr_(ptr) {}
  void Then(std::function<int(FutureWrapper<T>&)>&& cb) {
    return future_ptr_->Then(std::move(cb));
  }
  void Notify(T t) {
    if (future_ptr_) {
      return future_ptr_->Notify(t);
    }
  }
  bool TryGetValue(T& value) {
    if (future_ptr_) {
      return future_ptr_->TryGetValue(value);
    }
    return false;
  }
  T GetValue() const {
    if (future_ptr_) {
      return future_ptr_->GetValue();
    }
    return T();
  }

  operator bool() const { return future_ptr_ ? true : false; }

  bool IsDone() const { return future_ptr_ ? future_ptr_->IsDone() : false; }

 private:
  std::shared_ptr<FutureWrapperBase<T>> future_ptr_;
};

template <typename T>
class FutureWrapperBase
    : public std::enable_shared_from_this<FutureWrapperBase<T>> {
 public:
  explicit FutureWrapperBase(std::shared_future<T>&& future,
                             bool fast_forward = false)
      : future_holder_(std::move(future)), fast_forward_(fast_forward) {}

  void Then(std::function<int(FutureWrapper<T>&)>&& cb) {
    this->cb_ = std::move(cb);
    if (has_value_) {
      ExecFunc();
    }
  }
  void Notify(T t) {
    has_value_ = true;
    if (fast_forward_) {
      holder_value_ = t;
    }
    ExecFunc();
  }
  bool TryGetValue(T& value) {
    if (not has_value_) {
      return false;
    }
    value = fast_forward_ ? holder_value_ : future_holder_.get();
    return true;
  }
  T GetValue() {
    if (fast_forward_) {
      return holder_value_;
    }
    return future_holder_.get();
  }

  bool IsDone() { return has_value_; }

 protected:
  void ExecFunc() {
    if (!flag_.load() and cb_) {
      flag_.store(true);
      // exec callback
      auto f = FutureWrapper<T>(this->shared_from_this());
      try {
        cb_(f);
      } catch (...) {
      }
      {
        decltype(cb_) tmp;
        tmp.swap(cb_);
      }
    }
  }

 private:
  std::shared_future<T> future_holder_;
  std::function<int(FutureWrapper<T>&)> cb_;
  std::atomic<bool> has_value_{false};
  std::atomic<bool> flag_{false};
  T holder_value_;
  bool fast_forward_{false};
};

};  // namespace yapf

#endif  // SRC_PHASE_COMMON_H_

