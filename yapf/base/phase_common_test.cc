//
// phase_common测试用例
//
#include "yapf/base/phase_common.h"

#include <chrono>
#include <iostream>
#include <thread>

#include "gtest/gtest.h"

namespace yapf {

TEST(PhaseCommonTest, SatisfiedFuture) {
  using std::cout;
  using std::endl;
  yapf::PromiseWrapper<int> p;
  p.SetValue(23);
  yapf::FutureWrapper<int> f = p.GetFuture();
  EXPECT_TRUE(f.IsDone());
  EXPECT_EQ(f.GetValue(), 23);
  yapf::PromiseWrapper<int> mp = std::move(p);
  auto mf = mp.GetFuture();
  EXPECT_TRUE(mf.IsDone());
  EXPECT_EQ(mf.GetValue(), 23);
  // test fast forward
  {
    yapf::PromiseWrapper<int> p{true};
    p.SetValue(23);
    yapf::FutureWrapper<int> f = p.GetFuture();
    EXPECT_TRUE(f.IsDone());
    EXPECT_EQ(f.GetValue(), 23);
    yapf::PromiseWrapper<int> mp = std::move(p);
    auto mf = mp.GetFuture();
    EXPECT_TRUE(mf.IsDone());
    EXPECT_EQ(mf.GetValue(), 23);
  }
}

TEST(PhaseCommonTest, WaitedFuture) {
  using std::cout;
  using std::endl;
  yapf::PromiseWrapper<int> p;
  yapf::FutureWrapper<int> f = p.GetFuture();
  std::thread t1(
      [](auto p) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        p.SetValue(12);
      },
      std::move(p));
  f.Then([](yapf::FutureWrapper<int> &t) -> int {
    cout << "callback. f get t = " << t.GetValue() << endl;
    return 0;
  });
  EXPECT_FALSE(f.IsDone());
  t1.join();
  cout << "outer f get t = " << f.GetValue() << endl;
  EXPECT_EQ(f.GetValue(), 12);
}

TEST(PhaseCommonTest, StrToInt64) {
  std::string str1{"1.2"};
  std::string str2{"12"};
  int64_t value{};
  EXPECT_FALSE(StrToInt64(str1.c_str(), value));
  EXPECT_TRUE(StrToInt64(str2.c_str(), value));
  EXPECT_EQ(value, 12);
}

TEST(PhaseCommonTest, StrToIntDouble) {
  std::string str1{"1.2ss"};
  std::string str2{"12"};
  double value{};
  EXPECT_FALSE(StrToDouble(str1.c_str(), value));
  EXPECT_TRUE(StrToDouble(str2.c_str(), value));
  EXPECT_EQ(value, 12);
}

TEST(PhaseCommonTest, ParseExprs) {
  std::vector<std::string> exprs1;
  std::vector<std::pair<std::string, std::string>> pairs;
  std::vector<std::string> single_nodes;
  EXPECT_NE(ParseExprs(exprs1, pairs, single_nodes), 0);
  std::vector<std::string> exprs2{"a->b", "c ->d ", "a-> d", "f"};
  EXPECT_EQ(ParseExprs(exprs2, pairs, single_nodes), 0);
  EXPECT_EQ(pairs.size(), 3u);
  EXPECT_STREQ(pairs[0].first.c_str(), "a");
  EXPECT_STREQ(pairs[0].second.c_str(), "b");
  EXPECT_EQ(single_nodes.size(), 1u);
}

TEST(PhaseCommonTest, Join) {
  std::vector<int> vals{1, 2, 3, 4};
  auto ret = Join(vals, "++");
  EXPECT_STREQ(ret.c_str(), "1++2++3++4");
  std::vector<int> vals1{};
  ret = Join(vals1, "-");
  EXPECT_TRUE(ret.empty());
}

struct JoinFiledTestType {
  int a{1};
  int b{};
};

TEST(PhaseCommonTest, JoinField) {
  std::vector<JoinFiledTestType> vals(2, JoinFiledTestType());
  auto ret =
      JoinField<JoinFiledTestType, int>(vals, &JoinFiledTestType::a, "->");
  EXPECT_STREQ(ret.c_str(), "1->1");
}

TEST(PhaseCommonTest, SepStrString) {
  auto ret = SepString("1 +2 ++3", "+");
  EXPECT_EQ(ret.size(), 3u);
  EXPECT_EQ(ret[0], std::string_view("1 "));
  EXPECT_EQ(ret[1], std::string_view("2 "));
  EXPECT_EQ(ret[2], std::string_view("3"));
  ret = SepString("1", "+");
  EXPECT_EQ(ret.size(), 1u);
}

TEST(PhaseCommonTest, Trim) {
  auto ret = Trim(" abc ");
  EXPECT_EQ(ret, std::string_view("abc"));
  ret = Trim(" abc d");
  EXPECT_EQ(ret, std::string_view("abc d"));
}

TEST(PhaseCommonTest, PhaseConfigKeyParse) {
  PhaseConfigKey pck;
  pck.Parse("");
  EXPECT_TRUE(pck.name.empty());
  pck.Parse(
      "FooPhase(test_timeout:false,flow_control:false,flow_win_size:100,"
      "flow_limit:1000,flow_limit_delay:true,delay_timeout:4000,redo:false,"
      "redo_retry_interval:200,redo_retry_times:1)");
  EXPECT_STREQ(pck.name.c_str(), "FooPhase");
  EXPECT_EQ(pck.params.params.size(), 9u);
  EXPECT_EQ(pck.params["test_timeout"].bv, false);
  EXPECT_EQ(pck.params["flow_win_size"].iv, 100);
}

}  // namespace yapf
