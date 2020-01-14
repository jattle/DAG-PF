## YAPF

YAPF(Yet Another Procedure Framework) is a program precedure control framework, 
its design principle is using DAG to describe relationship between progam logic components.
Once DAG is built, during topology sorting on DAG, each logic component(Phase) is scheduled to execute 
just at the right time, you dont bother to schedule Phase A after Phase B by hard coding.
Of course, you must have to write scheduling code(serial and parallel combination) yourself
when using procedure control primitives such as promises / futures and coroutines, 
which often leads to poor readability.
    
Users only have to declare definition and relationship between each phase, then writing separate
implementation of each phase, execution order of each phase will be scheduled by this framework. 
    
Execution of phase is handled by procedure schedule thread, it provides several type, 
std::thread by default, and coroutine also cant be wrapped as a kind of scheduler thread, 
users can implement new subclass by inheriting class `SchedulerThreadBase`.
an example. 

This procedure framework is independent of any RPC framework, and is applicable to the internal 
process organization of any program.

Note: C++ 17 required.

## Description 

- yapf/base 
  
base source code, includes Processing of DAG、Phase Scheduler、Timer Logic etc.

- yapf/flow_control
implementation of sliding window flow control and utils.

- TODO
provide more examples。
