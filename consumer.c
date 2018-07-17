#include <string.h>
#include <stdlib.h>
#include <syslog.h>
#include <signal.h>
#include <error.h>
#include <getopt.h>
 
#include "src/rdkafka.h"

static rd_kafka_t *rk;
static rd_kafka_topic_partition_list_t *topics;
static void msg_consume (rd_kafka_message_t *rkmessage,
       void *opaque) {
  if (rkmessage->err) {
    if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
      fprintf(stderr,
        "%% Consumer reached end of %s [%"PRId32"] "
             "message queue at offset %"PRId64"\n",
             rd_kafka_topic_name(rkmessage->rkt),
             rkmessage->partition, rkmessage->offset);
 
      return;
    }
 
    if (rkmessage->rkt)
            fprintf(stderr, "%% Consume error for "
                    "topic \"%s\" [%"PRId32"] "
                    "offset %"PRId64": %s\n",
                    rd_kafka_topic_name(rkmessage->rkt),
                    rkmessage->partition,
                    rkmessage->offset,
                    rd_kafka_message_errstr(rkmessage));
    else
            fprintf(stderr, "%% Consumer error: %s: %s\n",
                    rd_kafka_err2str(rkmessage->err),
                    rd_kafka_message_errstr(rkmessage));
 
    if (rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION ||
        rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC)
          run = 0;
    return;
  }
 
  fprintf(stdout, "%% Message (topic %s [%"PRId32"], "
                      "offset %"PRId64", %zd bytes):\n",
                      rd_kafka_topic_name(rkmessage->rkt),
                      rkmessage->partition,
    rkmessage->offset, rkmessage->len);
 
  if (rkmessage->key_len) {
    printf("Key: %.*s\n",
             (int)rkmessage->key_len, (char *)rkmessage->key);
  }
 
  printf("%.*s\n",
           (int)rkmessage->len, (char *)rkmessage->payload);
  
}
int main(){
	char *brokers = "192.168.17.199:9092";
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
	rd_kafka_resp_err_t err;
	char errstr[512];
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
	while(true){
		rd_kafka_message_t *rkmessage;
		rkmessage = rd_kafka_consumer_poll(rk, 1000);
		msg_consume(rkmessage);
	}

}