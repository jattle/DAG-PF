// File Name: SlidingWindowCounter.h
// Author: jattlelin
// Created Time: 2018-07-28 18:21:42
// Description: 
// 窗口最大为1s的滑动窗口限流

#ifndef _SLIDINGWINDOWCOUNTER_H
#define _SLIDINGWINDOWCOUNTER_H


#include <mutex>
#include <shared_mutex>
#include <atomic>

#include <map>
#include <string>
#include <cstring>

constexpr auto kWindowArraySize = 10000;
constexpr auto kRetOK = 0;
constexpr auto kRetFlowLimit = 1;
constexpr auto kRetError = -1;

class SecondSlidingWindowCounter
{
	struct WindowElementValue
	{
		WindowElementValue() = default;
		std::atomic_ulong counter = {0};
		int64_t accessTime = {0};
	};

	struct SlidingWindowValue
	{
		WindowElementValue windowArr[kWindowArraySize];
		volatile int64_t windowStart = {-1};
		volatile int64_t windowEnd = {-1};
		volatile int64_t windowCursor = {-1};
		std::atomic_ulong counter = {0};
	};
public:
	explicit SecondSlidingWindowCounter(unsigned int windowSize, unsigned long maxFlowSize)
	{
		if(windowSize > 1u && windowSize <= 1000u) 
			m_windowSize = windowSize;
		if(maxFlowSize > 1u) 
			m_maxFlowSize = maxFlowSize;
	}
	SecondSlidingWindowCounter(const SecondSlidingWindowCounter&) = delete;
	SecondSlidingWindowCounter& operator=(const SecondSlidingWindowCounter&) = delete;
	int inc();
private:
	SlidingWindowValue slidingWindow;
    mutable std::mutex m_lock;
	unsigned int m_windowSize = {1000u};
	unsigned long m_maxFlowSize = {20000u};
};

#endif

