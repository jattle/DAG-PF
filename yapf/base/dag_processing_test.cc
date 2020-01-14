// File Name: dag_processing_test.cc
// Author: jattlelin
// Created Time: 2019-12-26 15:42:41
// Description:

#include "yapf/base/dag_processing.h"
#include "yapf/base/phase_common.h"

#include "gtest/gtest.h"

namespace yapf {

TEST(DAGProcessingTest, EmptyDAG) {
  DAG dag;
  auto ret = dag.Init([](const auto &t) -> bool { return true; });
  EXPECT_EQ(ret, kDagOpRetEmptyNodes);
}

TEST(DAGProcessingTest, NotConnectedDAG) {
  DAG dag;
  std::vector<std::pair<std::string, std::string> > pairs;
  std::vector<std::string> single_nodes;
  std::vector<std::string> exprs{"a->b", "b->c", "c->a", "d"};
  EXPECT_EQ(0, ParseExprs(exprs, pairs, single_nodes));
  std::unordered_map<std::string, std::string> alias_map;
  EXPECT_EQ(0, dag.AddNodeLinks(pairs, single_nodes, alias_map));
  auto ret = dag.Init([](const auto &t) -> bool { return true; });
  EXPECT_EQ(ret, kDagOpRetNotConnected);
}

TEST(DAGProcessingTest, CircleDAG) {
  DAG dag;
  std::vector<std::pair<std::string, std::string> > pairs;
  std::vector<std::string> single_nodes;
  std::vector<std::string> exprs{"a->b", "b->c", "c->d", "d->b", "e"};
  EXPECT_EQ(0, ParseExprs(exprs, pairs, single_nodes));
  std::unordered_map<std::string, std::string> alias_map;
  EXPECT_EQ(0, dag.AddNodeLinks(pairs, single_nodes, alias_map));
  auto ret = dag.Init([](const auto &t) -> bool { return true; });
  EXPECT_EQ(ret, kDagOpRetHasCircle);
}

TEST(DAGProcessingTest, NormalDAG) {
  DAG dag;
  std::vector<std::pair<std::string, std::string> > pairs;
  std::vector<std::string> single_nodes;
  std::vector<std::string> exprs{"a->b", "b->c", "b->d", "e"};
  EXPECT_EQ(0, ParseExprs(exprs, pairs, single_nodes));
  std::unordered_map<std::string, std::string> alias_map;
  EXPECT_EQ(0, dag.AddNodeLinks(pairs, single_nodes, alias_map));
  auto ret = dag.Init([](const auto &t) -> bool { return true; });
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(dag.Size(), 7u);
  EXPECT_STREQ(dag.GetStartNode()->GetFullName().c_str(), "StartPhase");
  EXPECT_STREQ(dag.GetEndNode()->GetFullName().c_str(), "EndPhase");
  // test pop
  std::vector<DAGNodePtr> top_nodes;
  EXPECT_EQ(0, dag.Pop(dag.GetStartNode(), top_nodes));
  // pop a & e
  EXPECT_EQ(top_nodes.size(), 2u);
  std::vector<DAGNodePtr> parents;
  // a & e all depend on StartNode
  EXPECT_EQ(0, dag.GetDepNodes(top_nodes[0], parents));
  EXPECT_EQ(parents.size(), 1u);
  EXPECT_STREQ(parents[0]->GetName().c_str(), "StartPhase");
}

}  // namespace yapf

