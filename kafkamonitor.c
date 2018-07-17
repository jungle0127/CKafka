#include <string.h>
#include <stdlib.h>
#include <syslog.h>
#include <signal.h>
#include <error.h>
#include <getopt.h>
#include <stdio.h>
#include "src/rdkafka.h"

static void msg_consume (rd_kafka_message_t *rkmessage,
       void *opaque) {
  if (rkmessage->key_len) {
    printf("==Key: %.*s\n",
             (int)rkmessage->key_len, (char *)rkmessage->key);
  }
 
  printf("==%.*s\n",
           (int)rkmessage->len, (char *)rkmessage->payload);
  
}
int main(){
    /**
     * common parameters&properties
     */ 
    char errstr[512];
    const char *brokers = "192.168.17.199:9092";

    /**
     * Parameters for producer.
     */ 
    rd_kafka_t *rk_producer;
    rd_kafka_topic_t *rkt_producer;
    rd_kafka_conf_t *conf_producer;
    /**
     * Temporary properties for Producer
     */ 
    char buf[512] = "producer message";    
    const char *topic_producer = "demo_topic";


    /**
     * Parameters for consumer
     */ 
    rd_kafka_t *rk_consumer;
    rd_kafka_topic_partition_list_t *topics_consumer;
    rd_kafka_conf_t *conf_consumer;
    rd_kafka_topic_conf_t *topic_conf_consumer;
    rd_kafka_resp_err_t err;

    /**
     * Properties for consumer
     */ 
    char *topic_consumer = "demo_topic";
    /////////////////////////////////////////////////////////
    // Producer configuration
    /////////////////////////////////////////////////////////

    /* Create Kafka client configuration place-holer*/
    conf_producer = rd_kafka_conf_new();
    /* Configure parameters of Kafka*/
    if(rd_kafka_conf_set(conf_producer,"bootstrap.servers",brokers,errstr,sizeof(errstr)) != RD_KAFKA_CONF_OK){
        fprintf(stderr,"%s\n",errstr);
        return 1;
    }

    /* create producer instance.*/
    rk_producer = rd_kafka_new(RD_KAFKA_PRODUCER,conf_producer,errstr,sizeof(errstr));
    if(!rk_producer){
        fprintf(stderr,"%% Failed to create new producer: %s\n",errstr);
        return 1;
    }
    /* Create topic instace for producer*/
    rkt_producer = rd_kafka_topic_new(rk_producer,topic_producer,NULL);
    if(!rkt_producer){
        fprintf(stderr,"%% Failed to create topic object for producer:%s\n",
            rd_kafka_err2str(rd_kafka_last_error()));
            return 1;
    }
    
    /////////////////////////////////////////////////////////
    // Consumer configuration
    /////////////////////////////////////////////////////////
    /* Create Kafka configuration for consumer*/
    conf_consumer = rd_kafka_conf_new();
    /* Create topic configuration for consumer topic*/
    topic_conf_consumer = rd_kafka_topic_conf_new();
    /* Configure Kafka consumer*/
    rd_kafka_conf_set(conf_consumer,"group.id","consumer.group",errstr,sizeof(errstr));
    /* Configure Kafka topic for consumer*/
    rd_kafka_topic_conf_set(topic_conf_consumer,"offset.store.method",
        "broker",errstr,sizeof(errstr));
    rd_kafka_conf_set_default_topic_conf(conf_consumer,topic_conf_consumer);
    /* Create Kafka instance for consumer*/
    rk_consumer = rd_kafka_new(RD_KAFKA_CONSUMER,conf_consumer,errstr,sizeof(errstr));
    /* Add broker list for consumer instance*/
    rd_kafka_brokers_add(rk_consumer,brokers);
    /* redirect rd_kafka_poll() queue to consumer_poll() queue*/
    rd_kafka_poll_set_consumer(rk_consumer);
    /* Create topic+partition space(list/vector)*/
    topics_consumer = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics_consumer,topic_consumer,-1);
    /* Start consumer subscribe*/
    rd_kafka_subscribe(rk_consumer,topics_consumer);

    /////////////////////////////////////////////////////////
    // loop for sending and receiving
    /////////////////////////////////////////////////////////

    for(int index = 0;index < 10;index++){
        int len = strlen(buf);
        int result = rd_kafka_produce(
            rkt_producer,
            RD_KAFKA_PARTITION_UA,
            RD_KAFKA_MSG_F_COPY,
            buf,len,
            NULL,0,NULL);
        rd_kafka_poll(rk_producer,0);
        rd_kafka_flush(rk_producer,10*1000);
        if(result > -1){		
		    fprintf(stderr, "%% Enqueued message (%d bytes) "
                                "for topic %s\n",
			len, rd_kafka_topic_name(rkt_producer));
	    }
        // send finished.
        rd_kafka_message_t *rkmessage;
        rkmessage = rd_kafka_consumer_poll(rk_consumer,1000);
        if(rkmessage){
            msg_consume(rkmessage,NULL);
            rd_kafka_message_destroy(rkmessage);
        }


        
    }
}
