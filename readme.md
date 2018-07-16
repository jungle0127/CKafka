# Kafka demo written by C

##Official website
http://kafka.apache.org/

## Official C library
https://github.com/edenhill/librdkafka

##为什么Kafka那么快
https://blog.csdn.net/z69183787/article/details/80323581

## Kafka producer&consumer 介绍
c语言使用librdkafka库实现kafka的生产和消费实例
https://blog.csdn.net/lijinqi1987/article/details/76582067

## C Kafka开发环境配置
Ubuntu16.04

1. 安装librdkafka
按照官网安装
./configure
make
make install

2. cp librdkafka/src到当前开发目录
3. cmd 环境变量配置
LD_LIBRARY_PATH=/usr/local/lib
export LD_LIBRARY_PATH
