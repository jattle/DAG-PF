// File Name: dag_processing.h
// Author: jattlelin
// Created Time: 2019-12-08 23:20:09
// Description: DAG图内部操作封装
//

#ifndef DAG_PROCESSING_H_
#define DAG_PROCESSING_H_

#include <atomic>
#include <bitset>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "yapf/base/logging.h"

namespace yapf {

enum EDAGOpRet {
  kDagOpRetEmptyLinks = 80000,
  kDagOpRetInvalidName,
  kDagOpRetNoStartEndNode,
  kDagOpRetHasCircle,
  kDagOpRetNotConnected,
  kDagOpRetEmptyNodes,
  kDagOpRetNoReadyNodes,
  kDagOpRetInvalidCopy,
};

class DAGNode {
  friend class DAG;

 public:
  explicit DAGNode(const std::string &node_name, uint32_t id)
      : id_(id), name_(node_name) {}
  DAGNode(const DAGNode &other) { CopyFrom(other); }
  DAGNode &operator=(const DAGNode &other) {
    if (this != &other) {
      CopyFrom(other);
    }
    return *this;
  }
  const std::string &GetName() const { return name_; }
  const std::string &GetFullName() const { return full_name_; }
  uint32_t GetId() const { return id_; }
  int GetIndegree() const { return indegree_.load(std::memory_order_relaxed); }
  int GetOutdegree() const { return links_.size(); }

 private:
  void CopyFrom(const DAGNode &other) {
    this->id_ = other.id_;
    this->name_ = other.name_;
    this->full_name_ = other.full_name_;
    this->indegree_.store(other.indegree_.load(std::memory_order_relaxed),
                          std::memory_order_relaxed);
    this->indegree_dup_.store(
        other.indegree_dup_.load(std::memory_order_relaxed),
        std::memory_order_relaxed);
    this->links_ = other.links_;
  }

 private:
  uint32_t id_{0};                //节点唯一id
  std::string name_;              //节点唯一名称
  std::string full_name_;         //节点全称
  std::atomic<int> indegree_{0};  //入度
  std::atomic<int> indegree_dup_{0};
  std::vector<uint32_t> links_;  //出边节点列表
};

using DAGNodePtr = std::shared_ptr<DAGNode>;

using NodeVisitor = std::function<int(DAGNodePtr node)>;

class DAG {
 public:
  DAG() = default;
  DAG(const DAG &other) = delete;
  DAG &operator=(const DAG &) = delete;
  ~DAG() = default;
  //加入连接关系,node1->node2，表示node2依赖node1
  int AddNodeLinks(
      const std::vector<std::pair<std::string, std::string>> &links,
      const std::vector<std::string> &single_nodes = std::vector<std::string>(),
      const std::unordered_map<std::string, std::string> &alias_name_map =
          std::unordered_map<std::string, std::string>());
  //弹出parent出节点中当前依赖已满足的节点
  int Pop(DAGNodePtr parent, std::vector<DAGNodePtr> &top_nodes);
  //对依赖关系预处理并判断有效性
  //输出拓扑排序结果
  int Init(auto &&valid_functor) {
    int ret = Adjust();
    if (ret != 0) {
      DAGPF_LOG_ERROR << "adjust failed." << std::endl;
      return ret;
    }
    // check validity
    ret = CheckValidity(valid_functor);
    if (ret != 0) {
      DAGPF_LOG_ERROR << "check validity failed: ret = " << ret << std::endl;
      return ret;
    }
    ret = Traverse();
    if (ret != 0) {
      DAGPF_LOG_ERROR << "traverse failed, maybe has circle. ret = " << ret
                      << std::endl;
      return ret;
    }
    return 0;
  }
  void List();
  //获取节点的依赖节点集合
  int GetDepNodes(DAGNodePtr node, std::vector<DAGNodePtr> &parents);
  DAGNodePtr GetStartNode() { return node_pool_[start_node_id_]; }
  DAGNodePtr GetEndNode() { return node_pool_[end_node_id_]; }
  int CopyFrom(const DAG &source);
  int TraverseAction(NodeVisitor functor) const;
  size_t Size() const { return node_pool_.size(); }
  void Clear();

 private:
  inline static const std::string kStartNodeName = "StartPhase";
  inline static const std::string kEndNodeName = "EndPhase";

 private:
  DAGNodePtr AllocNode(const std::string &);
  //检验DAG图有效性
  int CheckValidity(auto &&valid_functor) {
    bool has_alias = !node_alias_name_map_.empty();
    for (auto &node : node_pool_) {
      if (has_alias) {
        auto alias_iter = node_alias_name_map_.find(node->name_);
        if (alias_iter == node_alias_name_map_.end()) {
          DAGPF_LOG_ERROR << "cant find full name for alias: " << node->name_
                          << std::endl;
          return kDagOpRetInvalidName;
        }
        node->full_name_ = alias_iter->second;
      } else {
        node->full_name_ = node->name_;
      }
      // check if can create instance
      if (!valid_functor(node->full_name_)) {
        DAGPF_LOG_ERROR << "not registered, alias: " << node->name_
                        << ", full name: " << node->full_name_ << std::endl;
        return kDagOpRetInvalidName;
      }
    }
    return 0;
  }
  int Adjust();
  int Traverse();
  int AddLink(const std::string &pre_node_name,
              const std::string &next_node_name);
  int DFS(DAGNodePtr node);
  int InnerPop(DAGNodePtr parent, std::vector<DAGNodePtr> &topNodes);
  bool IsReservedName(const std::string &);

 private:
  using Name2NodeMap = std::unordered_map<std::string, uint32_t>;
  using StringSet = std::unordered_set<std::string>;
  using NodeAliasNameMap = std::unordered_map<std::string, std::string>;
  std::vector<DAGNodePtr> node_pool_;  //节点池
  Name2NodeMap node_name_map_;         //节点名称到id映射关系
  uint32_t allocated_node_id_{0};      //已分配节点序号
  NodeAliasNameMap node_alias_name_map_;
  bool has_traversed_{false};
  StringSet pair_set_;  // link去重
  inline static constexpr size_t kMaxDagNodeNum = 1024;
  std::bitset<kMaxDagNodeNum> node_visited_set_;       //是否已访问
  std::bitset<kMaxDagNodeNum> recur_stack_set_;        // dfs访问轨迹记录
  std::vector<std::vector<DAGNodePtr>> node_parents_;  //节点的依赖关系
  std::vector<std::vector<DAGNodePtr>> *node_parents_ptr_{
      nullptr};  //节点的依赖关系ptr
  uint32_t start_node_id_{0};
  uint32_t end_node_id_{0};
  // TODO modify copyFrom together
};

}  // namespace yapf

#endif

