// File Name: dag_processing.cpp
// Author: jattlelin
// Created Time: 2019-12-10 16:01:14
// Description:

#include "yapf/base/dag_processing.h"

#include <algorithm>
#include <cassert>

#include "yapf/base/logging.h"

namespace yapf {

bool DAG::IsReservedName(const std::string &name) {
  return name == DAG::kStartNodeName || name == DAG::kEndNodeName;
}

DAGNodePtr DAG::AllocNode(const std::string &name) {
  auto node = std::make_shared<DAGNode>(name, allocated_node_id_++);
  node_pool_.push_back(node);
  return node;
}

int DAG::AddNodeLinks(
    const std::vector<std::pair<std::string, std::string> > &links,
    const std::vector<std::string> &single_nodes,
    const std::unordered_map<std::string, std::string> &node_alias_name_map) {
  if (links.empty() && single_nodes.empty()) {
    DAGPF_LOG_ERROR << "both links and single nodes are empty!" << std::endl;
    return kDagOpRetEmptyLinks;
  }
  node_pool_.reserve(links.size() + single_nodes.size() + 2u);
  node_alias_name_map_.insert(node_alias_name_map.begin(),
                              node_alias_name_map.end());
  for (const auto &item : links) {
    if (IsReservedName(item.first) || IsReservedName(item.second)) {
      DAGPF_LOG_ERROR << "reserved name is not allowed for phase alias name."
                      << std::endl;
      return kDagOpRetInvalidName;
    }
    AddLink(item.first, item.second);
  }
  // add single node
  for (const auto &node_name : single_nodes) {
    if (IsReservedName(node_name)) {
      DAGPF_LOG_ERROR << "reserved name is not allowed for phase alias name."
                      << std::endl;
      return kDagOpRetInvalidName;
    }
    std::pair<Name2NodeMap::iterator, bool> node_iter =
        node_name_map_.emplace(node_name, 0u);
    if (node_iter.second) {
      auto new_node = AllocNode(node_name);
      node_iter.first->second = new_node->id_;
    } else {
      DAGPF_LOG_ERROR << "node " << node_name << " already created, ignore."
                      << std::endl;
    }
  }
  return 0;
}

int DAG::AddLink(const std::string &pre_node_name,
                 const std::string &next_node_name) {
  DAGPF_LOG_DEBUG << "add link " << pre_node_name << " -> " << next_node_name
                  << std::endl;
  std::string bundle = pre_node_name + "->" + next_node_name;
  if (pair_set_.count(bundle) != 0) {
    return 0;
  }
  pair_set_.insert(bundle);
  //
  std::pair<Name2NodeMap::iterator, bool> pre_node_iter =
      node_name_map_.emplace(pre_node_name, 0u);
  if (pre_node_iter.second) {
    auto new_node = AllocNode(pre_node_name);
    pre_node_iter.first->second = new_node->id_;
  }
  //
  std::pair<Name2NodeMap::iterator, bool> next_node_iter =
      node_name_map_.emplace(next_node_name, 0u);
  if (next_node_iter.second) {
    auto new_node = AllocNode(next_node_name);
    next_node_iter.first->second = new_node->id_;
  }
  // add node link
  node_pool_[pre_node_iter.first->second]->links_.push_back(
      next_node_iter.first->second);
  // inc indegree
  node_pool_[next_node_iter.first->second]->indegree_.fetch_add(
      1, std::memory_order_relaxed);
  return 0;
}

// pop parent's children which indegree is 0
int DAG::Pop(DAGNodePtr parent, std::vector<DAGNodePtr> &top_nodes) {
  for (const auto &item : parent->links_) {
    auto node = node_pool_[item];
    if (node->indegree_.fetch_sub(1, std::memory_order_relaxed) == 1) {
      top_nodes.push_back(node);
    }
  }
  return top_nodes.empty() ? kDagOpRetNoReadyNodes : 0;
}

int DAG::InnerPop(DAGNodePtr parent, std::vector<DAGNodePtr> &top_nodes) {
  top_nodes.clear();
  for (const auto &id : parent->links_) {
    auto node = node_pool_[id];
    if (node->indegree_dup_.fetch_sub(1, std::memory_order_relaxed) == 1) {
      top_nodes.push_back(node);
    }
  }
  return 0;
}

int DAG::Adjust() {
  if (node_pool_.empty()) {
    DAGPF_LOG_ERROR << "empty nodes." << std::endl;
    return kDagOpRetEmptyNodes;
  }
  // select start nodes and end nodes
  std::vector<DAGNodePtr> start_nodes, end_nodes;
  for (auto &node : node_pool_) {
    if (node->indegree_.load(std::memory_order_relaxed) == 0) {
      start_nodes.push_back(node);
    }
    if (node->links_.empty()) {
      end_nodes.push_back(node);
    }
  }
  // add StartPhase and EndPhase
  if (start_nodes.empty() || end_nodes.empty()) {
    DAGPF_LOG_ERROR << "empty start nodes or end nodes." << std::endl;
    return kDagOpRetNoStartEndNode;
  }
  for (auto &node : start_nodes) {
    AddLink(DAG::kStartNodeName, node->name_);
  }
  for (auto &node : end_nodes) {
    AddLink(node->name_, DAG::kEndNodeName);
  }
  // set start node
  start_node_id_ = node_name_map_[DAG::kStartNodeName];
  node_pool_[start_node_id_]->full_name_ = DAG::kStartNodeName;
  end_node_id_ = node_name_map_[DAG::kEndNodeName];
  node_pool_[end_node_id_]->full_name_ = DAG::kEndNodeName;
  // update node alias map if necessary
  if (!node_alias_name_map_.empty() &&
      node_alias_name_map_.find(DAG::kStartNodeName) ==
          node_alias_name_map_.end()) {
    node_alias_name_map_.emplace(DAG::kStartNodeName, DAG::kStartNodeName);
  }
  if (!node_alias_name_map_.empty() &&
      node_alias_name_map_.find(DAG::kEndNodeName) ==
          node_alias_name_map_.end()) {
    node_alias_name_map_.emplace(DAG::kEndNodeName, DAG::kEndNodeName);
  }
  return 0;
}

// detect circle, collect node parents
int DAG::Traverse() {
  node_parents_.resize(kMaxDagNodeNum);
  if (has_traversed_) return 0;
  int ret = DFS(node_pool_[start_node_id_]);
  if (ret != 0) return ret;
  if (node_visited_set_.count() != node_name_map_.size()) {
    DAGPF_LOG_ERROR << "visited node count: " << node_visited_set_.count()
                    << ", all node count: " << node_name_map_.size()
                    << std::endl;
    return kDagOpRetNotConnected;
  }
  node_parents_ptr_ = &node_parents_;
  has_traversed_ = true;
  return 0;
}

int DAG::DFS(DAGNodePtr node) {
  // TODO
  node_visited_set_.set(node->id_);
  recur_stack_set_.set(node->id_);
  for (const auto &link_id : node->links_) {
    if (node_visited_set_.test(link_id)) {
      if (recur_stack_set_.test(link_id)) {
        DAGPF_LOG_ERROR << "circle detected between node "
                        << node_pool_[link_id]->name_ << "->" << node->name_
                        << std::endl;
        return kDagOpRetHasCircle;
      }
      node_parents_[link_id].push_back(node_pool_[node->id_]);
      continue;
    }
    node_parents_[link_id].push_back(node_pool_[node->id_]);
    int ret = DFS(node_pool_[link_id]);
    if (ret != 0) {
      return ret;
    }
  }
  recur_stack_set_.set(node->id_, false);
  return 0;
}


// display topology sort result
void DAG::List() {
  assert(start_node_id_ != 0);
  // restore indegreeDup
  for_each(node_pool_.begin(), node_pool_.end(), [](auto &item) {
    item->indegree_dup_.store(item->indegree_.load(std::memory_order_relaxed),
                              std::memory_order_relaxed);
  });
  std::vector<DAGNodePtr> parent_nodes;
  parent_nodes.push_back(node_pool_[start_node_id_]);
  int level = 1;
  do {
    DAGPF_LOG_DEBUG << "level " << level++ << std::endl;
    for (const auto &node : parent_nodes) {
      DAGPF_LOG_DEBUG << "\tnode " << node->name_ << ", indegree: "
                      << node->indegree_.load(std::memory_order_relaxed)
                      << ", outdegree: " << node->links_.size() << std::endl;
    }
    std::vector<DAGNodePtr> top_nodes;
    for (auto &node : parent_nodes) {
      std::vector<DAGNodePtr> tmp_nodes;
      InnerPop(node, tmp_nodes);
      if (!tmp_nodes.empty()) {
        top_nodes.insert(top_nodes.end(), tmp_nodes.begin(), tmp_nodes.end());
      }
    }
    parent_nodes.swap(top_nodes);
  } while (!parent_nodes.empty());
}

int DAG::GetDepNodes(DAGNodePtr node, std::vector<DAGNodePtr> &parents) {
  parents = (*node_parents_ptr_)[node->id_];
  return 0;
}

int DAG::CopyFrom(const DAG &source) {
  if (!source.has_traversed_) {
    DAGPF_LOG_ERROR << "DAG not traversed, cant copy." << std::endl;
    return kDagOpRetInvalidCopy;
  }
  this->node_pool_ = source.node_pool_;
  this->has_traversed_ = source.has_traversed_;
  this->node_parents_ptr_ = source.node_parents_ptr_;
  this->start_node_id_ = source.start_node_id_;
  this->end_node_id_ = source.end_node_id_;
  for (auto &node : node_pool_) {
    auto new_node = std::make_shared<DAGNode>(node->name_, node->id_);
    *new_node = *node;
    node = new_node;
  }
  return 0;
}

int DAG::TraverseAction(NodeVisitor functor) const {
  for (auto &node : node_pool_) {
    int ret = (functor)(node);
    if (ret != 0) {
      return ret;
    }
  }
  return 0;
}

void DAG::Clear() {
  allocated_node_id_ = 0;
  has_traversed_ = false;
  node_parents_ptr_ = nullptr;
  start_node_id_ = 0;
  end_node_id_ = 0;
  pair_set_.clear();
  node_visited_set_.reset();
  recur_stack_set_.reset();
  node_parents_.clear();
  node_name_map_.clear();
  node_alias_name_map_.clear();
  node_pool_.clear();
}

}  // namespace yapf
