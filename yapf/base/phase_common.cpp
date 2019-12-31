// File Name: phase_common.cpp
// Author: jattlelin
// Created Time: 2019-12-07 16:34:46
// Description:

#include "yapf/base/phase_common.h"

namespace yapf {

bool StrToInt64(const char *str, int64_t &value) {
  char *endptr{nullptr};
  long long int val = strtoll(str, &endptr, 10);
  if (nullptr == endptr || 0 != *endptr) {
    return false;
  }
  value = val;
  return true;
}

bool StrToDouble(const char *str, double &value) {
  char *endptr{nullptr};
  double val = strtod(str, &endptr);
  if (nullptr == endptr || 0 != *endptr) {
    return false;
  }
  value = val;
  return true;
}

int ParseExprs(const std::vector<std::string> &exprs,
               std::vector<std::pair<std::string, std::string> > &pairs,
               std::vector<std::string> &single_nodes, const std::string &sep) {
  pairs.clear();
  single_nodes.clear();
  for (auto iter = exprs.begin(); iter != exprs.end(); ++iter) {
    auto segs = SepString(*iter, sep);
    if (segs.size() == 1u) {
      auto svt = Trim(segs.at(0));
      std::string node_name(svt.data(), svt.size());
      if (!node_name.empty()) {
        single_nodes.push_back(std::move(node_name));
      }
    } else if (segs.size() == 2u) {
      auto svt0 = Trim(segs.at(0));
      auto svt1 = Trim(segs.at(1));
      pairs.push_back({std::string{svt0.data(), svt0.size()},
                       std::string(svt1.data(), svt1.size())});
    }
  }
  if (single_nodes.empty() && pairs.empty()) return 1;
  return 0;
}

std::vector<std::string_view> SepString(std::string_view sv,
                                        const std::string_view& delim) {
  std::vector<std::string_view> ret;
  while (true) {
    auto index = sv.find_first_of(delim);
    if (index != std::string_view::npos) {
      // maybe have empty item
      if (index != 0) {
        auto svt = sv.substr(0, index);
        ret.push_back(std::move(svt));
      }
      sv = sv.substr(index + 1);
    } else {
      ret.push_back(sv);
      break;
    }
  }
  return ret;
}

std::string_view Trim(std::string_view str) {
  static const char space_chars[] = {'\r', '\n', '\t', ' '};
  auto is_space_characters = [](auto c) {
    for (const auto& c_ : space_chars) {
      if (c == c_) return true;
    }
    return false;
  };
  std::string_view ret = str;
  // trim left
  while (!ret.empty() && is_space_characters(*ret.cbegin())) {
    ret.remove_prefix(1u);
  }
  // trim right
  while (!ret.empty() && is_space_characters(*ret.rbegin())) {
    ret.remove_suffix(1u);
  }
  return ret;
}


}
