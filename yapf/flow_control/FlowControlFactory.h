// File Name: FlowControlFactory.h
// Author: jattlelin
// Created Time: 2018-08-16 17:05:50
// Description: 

#ifndef _FLOWCONTROLFACTORY_H
#define _FLOWCONTROLFACTORY_H

#include <atomic>
#include <mutex>
#include <unordered_map>
#include <memory>

#include "yapf/flow_control/SlidingWindowCounter.h"
#include "yapf/flow_control/FlowControl.h"

//
//FLOW_WIN_SIZE: ms级别
//FLOW_LIMIT: 当前时间窗口内允许的流量大小 
//
class FlowControlFactory
{
		FlowControlFactory(){} 
		FlowControlFactory(const FlowControlFactory&) = delete;
		FlowControlFactory& operator=(const FlowControlFactory&) = delete;
	public:
		static FlowControlFactory* getInstance()
		{
			static std::mutex s_mutex;
			FlowControlFactory *tmp = s_instance.load();
			if(tmp == nullptr)
			{
				std::unique_lock<std::mutex> locker(s_mutex);
				tmp = s_instance.load();
				if(tmp == nullptr)
				{
					tmp = new FlowControlFactory();
					s_instance.store(tmp);
				}
			}
			return tmp;
		}
		
		//默认创建一个10ms内最大流量为100的限流器，即1w/s
		template<typename ControlType=SecondSlidingWindowCounter>
		std::shared_ptr<FlowController<ControlType> > getFlowController(const std::string &name, size_t flowWinSize=10, size_t flowLimit=100)
		{
			static std::unordered_map<std::string, std::shared_ptr<FlowController<ControlType> > > s_controllerMap;
			static std::shared_timed_mutex s_rwLock;
			{
				std::shared_lock<std::shared_timed_mutex> rlocker(s_rwLock);
				auto mIter = s_controllerMap.find(name);
				if(mIter != s_controllerMap.end())
				{
					return mIter->second;
				}
			}
			std::unique_lock<std::shared_timed_mutex> wlocker(s_rwLock);
			auto mIter = s_controllerMap.find(name);
			if(mIter != s_controllerMap.end())
			{
				return mIter->second;
			}
			std::shared_ptr<FlowController<ControlType> > tmp 
				= std::make_shared<FlowController<ControlType> >(flowWinSize, flowLimit);
			auto ret = s_controllerMap.emplace(name, tmp);
			return ret.first->second;
		}

	private:
		static std::atomic<FlowControlFactory*> s_instance;
};
#endif

