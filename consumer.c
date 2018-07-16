#include <string.h>
#include <stdlib.h>
#include <syslog.h>
#include <signal.h>
#include <error.h>
#include <getopt.h>
 
#include "src/rdkafka.h"

static rd_kafka_t *rk;
static rd_kafka_topic_partition_list_t *topics;
int main(){
	char *brokers = "192.168.17.199:9092";
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
	// 创建kafka配置
	conf = rd_kafka_conf_new();
	// 创建topic配置
	topic_conf = rd_kafka_topic_conf_new();
	// 配置kafka各项参数
	// TODO:
	rd_kafka_conf_set(conf, "debug", debug, errstr, sizeof(errstr);
	// 配置kafka topic各项参数
	// TODO:
	rd_kafka_topic_conf_set(topic_conf, "offset.store.method",
                                            "broker",errstr, sizeof(errstr);
	// 创建consumer实例					    
	rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf,errstr, sizeof(errstr));
	// 为consumer实例添加brokerlist
	rd_kafka_brokers_add(rk, brokers);
	// 开启consumer订阅
	rd_kafka_subscribe(rk, topics);
	// 轮询消息或事件，并调用回调函数
	// TODO: 无限轮询
	rd_kafka_message_t *rkmessage;
	rkmessage = rd_kafka_consumer_poll(rk, 1000)

}