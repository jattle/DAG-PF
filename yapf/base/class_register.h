// File Name: class_register.h
// Author: jattlelin
// Created Time: 2015-10-11 10:14:25
// Description: 
/// 通用模板实现类注册机制
/// 根据派生类名称从基类对象工厂取出任意派生类指针
/// 使用类名绑定仿函数方式实现惰性加载
/// 注意: 类的注册是在全局静态初始化实现的，
/// 带有注册代码的目标文件必须强制链接，不能打包成静态库
///

#ifndef _CLASS_REGISTER_H
#define _CLASS_REGISTER_H


#include <cstring>
#include <map>
#include <string>
#include <cassert>
#include <algorithm>



template<typename Base>
class GenObjectFun
{
    public:
    virtual Base* operator()()
    {
        return NULL;
    }
};


template<typename Base>
std::map<std::string, GenObjectFun<Base>* >& GetBaseMap()
{
   static std::map<std::string, GenObjectFun<Base>* > base_map;
   return base_map;
}

template<typename T>
class CreateNew
{
    public:
        static T* create() {return new T;}
};

template<typename T>
class CreateStatic
{
    public:
        static T* create() {static T t; return &t;}
};


//注册实例生成器

#define REGISTER_CLASS_BASE(BASE_NAMESPACE, BASE_NAME, CLASS_NAMESPACE, CLASS_NAME, CREATE_POLICY)\
template<template<class> class CreatePolicy = CREATE_POLICY> \
class Gen##CLASS_NAMESPACE##CLASS_NAME: public GenObjectFun<BASE_NAMESPACE::BASE_NAME>\
{\
    public:\
           BASE_NAMESPACE::BASE_NAME* operator()()\
  {\
      return CreatePolicy< CLASS_NAMESPACE::CLASS_NAME>::create();\
  }\
};\
\
static struct CLASS_NAMESPACE##CLASS_NAME##Initializer\
{\
    CLASS_NAMESPACE##CLASS_NAME##Initializer()\
    {\
      if(GetBaseMap<BASE_NAMESPACE::BASE_NAME>().find(#CLASS_NAMESPACE"."#CLASS_NAME) == GetBaseMap<BASE_NAMESPACE::BASE_NAME>().end())\
        {\
                GetBaseMap<BASE_NAMESPACE::BASE_NAME>().insert(std::make_pair(#CLASS_NAMESPACE"."#CLASS_NAME, new Gen##CLASS_NAMESPACE##CLASS_NAME<CREATE_POLICY>())); \
        }\
    }\
} __##CLASS_NAMESPACE##CLASS_NAME##Init;


template<typename Base>
Base* CreateObject(const std::string &class_namespace, const std::string& class_name)
{
    typename std::map<std::string, GenObjectFun<Base>* >::const_iterator iter
        = GetBaseMap<Base>().find(class_namespace + "." + class_name);
    if(iter == GetBaseMap<Base>().end())
    {
        return NULL;
    }
    return (*iter->second)();
}

template<typename Base>
Base* CreateObject(const std::string& class_name)
{
    typename std::map<std::string, GenObjectFun<Base>* >::const_iterator iter = GetBaseMap<Base>().find(class_name);
    if(iter == GetBaseMap<Base>().end())
    {
        return NULL;
    }
    return (*iter->second)();
}

template<typename Base>
bool HasRegisted(const std::string &class_namespace, const std::string& class_name)
{
    typename std::map<std::string, GenObjectFun<Base>* >::const_iterator iter
        = GetBaseMap<Base>().find(class_namespace + "." + class_name);
    return iter != GetBaseMap<Base>().end();
}


//指定命名空间、基类名称、子类名称注册对象生成器
//默认使用动态分配方式(CreateNew)生成对象
//可以改为采用静态分配方式(CreateStatic)
//创建及使用对象时需要注意注册时采用的分配方式
#define REGISTER_CLASS(BASE_NAMESPACE, BASE_NAME, CLASS_NAMESPACE, CLASS_NAME) \
        REGISTER_CLASS_BASE(BASE_NAMESPACE, BASE_NAME, CLASS_NAMESPACE, CLASS_NAME, CreateNew)

//指定完全名称访问，BASE_NAME格式为namespace::base_class_name, CLASS_NAME格式为namespace.class_name
#define CREATE_OBJECT(FULL_BASE_NAME, FULL_CLASS_NAME) \
    CreateObject<FULL_BASE_NAME>(FULL_CLASS_NAME)

//分拆名称，指定命名空间访问
#define CREATE_OBJECT_BASE(BASE_NAMESPACE, BASE_NAME, CLASS_NAMESPACE, CLASS_NAME)\
    CreateObject<BASE_NAMESPACE::BASE_NAME>(#CLASS_NAMESPACE, #CLASS_NAME)
#endif

